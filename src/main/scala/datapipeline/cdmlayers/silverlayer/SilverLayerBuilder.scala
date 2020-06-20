package datapipeline.cdmlayers.silverlayer

import datapipeline.consumer.KafkaConsumer
import hudi.basics.QueryData.spark
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead, lit, max, when}
import utilities.{ConfigurationFactory, ConfigurationHelper, SparkFactory}
import utilities.Constants._
import utilities.ColumnConstants._


class SilverLayerBuilder(configurationHelper:ConfigurationHelper,spark:SparkSession){


  import spark.implicits._
  def calculateVolumnRiseOrDrop(data:DataFrame) = {
    val windowBySymbolOrderByTime= Window.partitionBy(ITEM).orderBy(col(INVENTORY_TIME))
    val volumeOverPeriod = data.withColumn(NEXT_QUANTITY, lead(QUANTITY, 1) over (windowBySymbolOrderByTime))
      .withColumn(NEXT_QUANTITY,when(col(NEXT_QUANTITY).isNull,col(QUANTITY)).otherwise(col(NEXT_QUANTITY)))
    val volumeDifference = volumeOverPeriod.withColumn(QUANTITY_DIFFERENCE,
      col(QUANTITY) - col(NEXT_QUANTITY))
    volumeDifference.withColumn(QUANTITY_RISE_OR_DROP,
      when(col(QUANTITY_DIFFERENCE) > 0,"rise").
        when(col(QUANTITY_DIFFERENCE) === 0,"no_change")
        .otherwise("drop"))
  }


  def performCalculations(existingData:DataFrame, newData :DataFrame) ={
    if(existingData.isEmpty) {
       calculateVolumnRiseOrDrop(newData.filter(col(ITEM)===lit(MILK)))
    }
     else {
      val symbolWithMaxTime = existingData.groupBy(col(ITEM).alias("existingSymbol"))
        .agg(max(INVENTORY_TIME).alias("maxTime"))
      val incrementalRawDataForExistingSymbols = symbolWithMaxTime.join(newData,
        ( col(ITEM)===col("existingSymbol")
          &&
          col(INVENTORY_TIME)>col("maxTime")
          )
      ).drop("maxTime","existingSymbol")
      val incrementalRawDataForNewSymbols = newData.join(symbolWithMaxTime.drop("maxTime"),
        col("existingSymbol") === col(ITEM),"left_anti").drop("existingSymbol")

      val exitingDataWithoutVolumnRaiseColumns = existingData.drop(NEXT_QUANTITY,QUANTITY_DIFFERENCE,QUANTITY_RISE_OR_DROP)
      val unionData = exitingDataWithoutVolumnRaiseColumns
                                            .union(incrementalRawDataForExistingSymbols)
                                            .union(incrementalRawDataForNewSymbols)
       calculateVolumnRiseOrDrop(unionData)
     }
  }

  def mergeDataWithSilverLayer(dataToWrite: DataFrame, silverLayerPath: String): Unit ={

    val updateMatchCondition = s"existingData.${ITEM} = newDataToWrite.${ITEM} " +
      s"AND existingData.${INVENTORY_TIME} = newDataToWrite.${INVENTORY_TIME} " +
      s"AND existingData.${NEXT_QUANTITY} != newDataToWrite.${NEXT_QUANTITY}"

    DeltaTable.forPath(spark, silverLayerPath)
      .as("existingData")
      .merge(
        dataToWrite.as("newDataToWrite"),updateMatchCondition
        )
      .whenMatched
      .updateExpr(
        Map(NEXT_QUANTITY -> s"newDataToWrite.${NEXT_QUANTITY}",
          QUANTITY_DIFFERENCE -> s"newDataToWrite.${QUANTITY_DIFFERENCE}",
          QUANTITY_RISE_OR_DROP -> s"newDataToWrite.${QUANTITY_RISE_OR_DROP}"))
      .execute()


    val insertMatchCondition = s"existingData.${ITEM} == newDataToWrite.${ITEM} AND existingData.${INVENTORY_TIME} == newDataToWrite.${INVENTORY_TIME}"
    DeltaTable.forPath(spark, silverLayerPath)
      .as("existingData")
      .merge(
        dataToWrite.as("newDataToWrite"),insertMatchCondition
      )
      .whenNotMatched().insertAll()
      .execute()



  }


  def dumpExistingData(rawDataPath:String,silverLayerPath:String): Unit ={

    val rawData = spark.read
                       .format(DELTA).load(rawDataPath)
    val dataWithVolumnRiseAndDrop = calculateVolumnRiseOrDrop(rawData.filter(col(ITEM)===lit(MILK)).filter(col(HOUR)===9))
    dataWithVolumnRiseAndDrop.write.format(DELTA)
      .partitionBy(DATE,HOUR)
      .mode(OVERWRITE).save(silverLayerPath)
  }


  def incrementalData(rawDataPath:String,silverLayerPath:String): Unit ={

    val silverLayerExistingData = spark.read.format(DELTA).load(silverLayerPath)
    val rawData = spark.read.format(DELTA).load(rawDataPath)
    val dataToWrite =performCalculations(silverLayerExistingData,rawData)
    mergeDataWithSilverLayer(dataToWrite,silverLayerPath)

  }

}


object SilverLayerBuilder {

  def main(args: Array[String]): Unit = {
    val filePath =  args(0)
    val loadData = args(1)
    val spark = SparkFactory.getSparkSession()
    val configurationHelper = ConfigurationFactory.getConfiguration(filePath)
    val silverLayerBuilder = new SilverLayerBuilder(configurationHelper,spark)

    val rawDataPath = configurationHelper.getString("rawPath")
    val silverLayerPath = configurationHelper.getString("silverLayerPath")

     if(loadData.equalsIgnoreCase("incremental")){
      silverLayerBuilder.incrementalData(rawDataPath,silverLayerPath)
     }
      else
      {
        silverLayerBuilder.dumpExistingData(rawDataPath,silverLayerPath)
      }
    val silverLayerData = spark.read.format(DELTA).load(silverLayerPath)
    println("Existing Number of records " + silverLayerData.count)
    silverLayerData.orderBy(ITEM,INVENTORY_TIME).show(200,false)

  }


}

