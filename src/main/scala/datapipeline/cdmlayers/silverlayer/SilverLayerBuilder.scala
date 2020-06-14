package datapipeline.cdmlayers.silverlayer

import datapipeline.consumer.KafkaConsumer
import hudi.basics.QueryData.spark
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead, lit, max, when}
import utilities.{ConfigurationFactory, ConfigurationHelper, SparkFactory}
import utilities.Constants._


class SilverLayerBuilder(configurationHelper:ConfigurationHelper,spark:SparkSession){


  import spark.implicits._
  def calculateVolumnRiseOrDrop(data:DataFrame) = {
    val windowBySymbolOrderByTime= Window.partitionBy("symbol").orderBy($"time")
    val volumeOverPeriod = data.withColumn("next_volume", lead("volume", 1) over (windowBySymbolOrderByTime))
      .withColumn("next_volume",when(col("next_volume").isNull,col("volume")).otherwise(col("next_volume")))
    val volumeDifference = volumeOverPeriod.withColumn("volume_difference",
      col("volume") - col("next_volume"))
    volumeDifference.withColumn("volume_rise_or_drop",
      when(col("volume_difference") > 0,"rise").
        when(col("volume_difference") === 0,"no_change")
        .otherwise("drop"))
  }


  def performCalculations(existingData:DataFrame, newData :DataFrame) ={
    if(existingData.isEmpty) {
       calculateVolumnRiseOrDrop(newData.filter($"symbol"===lit("BAND")))
    }
     else {
      val symbolWithMaxTime = existingData.groupBy($"symbol".alias("existingSymbol"))
        .agg(max("time").alias("maxTime"))
      val incrementalRawDataForExistingSymbols = symbolWithMaxTime.join(newData,
        ( col("symbol")===col("existingSymbol")
          &&
          col("time")>col("maxTime")
          )
      ).drop("maxTime","existingSymbol")
      val incrementalRawDataForNewSymbols = newData.join(symbolWithMaxTime.drop("maxTime"),
        col("existingSymbol") === col("symbol"),"left_anti").drop("existingSymbol")

      val exitingDataWithoutVolumnRaiseColumns = existingData.drop("next_volume","volume_difference","volume_rise_or_drop")
      val unionData = exitingDataWithoutVolumnRaiseColumns
                                            .union(incrementalRawDataForExistingSymbols)
                                            .union(incrementalRawDataForNewSymbols)
       calculateVolumnRiseOrDrop(unionData)
     }
  }

  def mergeDataWithSilverLayer(dataToWrite: DataFrame, silverLayerPath: String): Unit ={

    val updateMatchCondition = "existingData.symbol = newDataToWrite.symbol" +
      " AND existingData.time = newDataToWrite.time " +
      "AND existingData.next_volume != newDataToWrite.next_volume"

    DeltaTable.forPath(spark, silverLayerPath)
      .as("existingData")
      .merge(
        dataToWrite.as("newDataToWrite"),updateMatchCondition
        )
      .whenMatched
      .updateExpr(
        Map("next_volume" -> "newDataToWrite.next_volume",
          "volume_difference" -> "newDataToWrite.volume_difference",
    "volume_rise_or_drop" -> "newDataToWrite.volume_rise_or_drop"))
      .execute()


    val insertMatchCondition = "existingData.symbol == newDataToWrite.symbol AND existingData.time == newDataToWrite.time"
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

    val dataWithVolumnRiseAndDrop = calculateVolumnRiseOrDrop(rawData.filter($"symbol"===lit("BAND")))
    dataWithVolumnRiseAndDrop.write.format(DELTA)
      .partitionBy("date","hour")
      .mode(OVERWRITE).save(silverLayerPath)
  }


  def incrementalData(rawDataPath:String,silverLayerPath:String): Unit ={

    val silverLayerExistingData = spark.read.format(DELTA).load(silverLayerPath)
    val rawData = spark.read.format(DELTA).load(rawDataPath)
      .filter($"symbol"===lit("BAND") or $"symbol"===lit("GOOG"))
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
    silverLayerData.orderBy("symbol","time").show(200,false)

  }


}

