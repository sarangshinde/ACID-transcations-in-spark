package datapipeline.cdmlayers.goldlayer

import hudi.basics.DataGenerator.hudiWriterConfig
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, max, row_number, sum}
import org.apache.spark.sql.types.StringType
import utilities.Constants.{DELTA, OVERWRITE}
import utilities.{ConfigurationFactory, ConfigurationHelper, SparkFactory}
import utilities.Constants._

class GoldLayerBuilder (configurationHelper:ConfigurationHelper,spark:SparkSession){

  import spark.implicits._

  def hudiWriterConfig(df:DataFrame) = {
    df.write.format(HUDI).
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "last_updated_at").
      option(RECORDKEY_FIELD_OPT_KEY, "symbol").
      option(PARTITIONPATH_FIELD_OPT_KEY, "date").
      option(HIVE_TABLE_OPT_KEY, HUDI_TABLENAME).
      option(HIVE_PARTITION_FIELDS_OPT_KEY, "date").
      option(HIVE_PARTITION_FIELDS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName).
      option(TABLE_NAME, HUDI_TABLENAME).
      option(OPERATION_OPT_KEY, UPSERT_OPERATION_OPT_VAL)
  }

  def performAggregationsUsingPartitonFor(incrementalSilverDataForExistingSymbols:DataFrame):DataFrame={

    val windowPartionByAppIdWithEventDescOrder: WindowSpec =
      Window.partitionBy("silverLayerData.symbol","silverLayerData.date")
        .orderBy(col("time").desc)

    val aggregatedData = incrementalSilverDataForExistingSymbols.withColumn("agg_volume",
      sum("volume") over windowPartionByAppIdWithEventDescOrder )
      .withColumn("max_time",
        max("time") over windowPartionByAppIdWithEventDescOrder )
      .withColumn("row_number",row_number over  windowPartionByAppIdWithEventDescOrder)
      .filter($"row_number"===1)

    val updateExistingAggregations= aggregatedData
      .withColumn("total_volume",
        col("goldLayerData.total_volume") + col("agg_volume"))
      .withColumn("last_updated_at",col("max_time"))
    updateExistingAggregations.select("silverLayerData.symbol","silverLayerData.date","silverLayerData.hour","total_volume","last_updated_at")

  }

  def performAggregations(data:DataFrame):DataFrame={

    data.groupBy("symbol","date","hour")
      .agg(sum("volume").alias("total_volume"),
           max("time").as("last_updated_at"))
      .withColumn("date",$"date".cast(StringType))
  }

  def dumpExistingData(silverLayerPath:String,goldLayerPath:String): Unit ={

    val silverLayerData = readData(silverLayerPath).filter($"symbol"===lit("BAND"))
    val aggregatedData = performAggregations(silverLayerData)
    hudiWriterConfig(aggregatedData)
      .mode(Append).save(goldLayerPath)
  }


  def incrementalData(silverLayerPath:String,goldLayerPath:String): Unit ={

    val goldLayerData = readData(goldLayerPath+"/*/*").alias("goldLayerData")
    val silverLayerData = readData(silverLayerPath)
      .select("symbol","date","hour","volume","time").alias("silverLayerData")

    val incrementalSilverDataForExistingSymbols = goldLayerData.join(silverLayerData,
      ( col("goldLayerData.symbol")===col("silverLayerData.symbol")
        &&
        col("time")>col("last_updated_at")
        )
    ).select("goldLayerData.last_updated_at","goldLayerData.total_volume","silverLayerData.*")

    val incrementalAggregatedDataExistingSymbol = performAggregationsUsingPartitonFor(incrementalSilverDataForExistingSymbols)

    val incrementalDataForNewSymbols = silverLayerData.join(goldLayerData.select("goldLayerData.symbol"),
      col("goldLayerData.symbol") === col("silverLayerData.symbol"),"left_anti")
      .drop("goldLayerData.symbol")

   val incrementalAggregatedDataNewSymbols  =  performAggregations(incrementalDataForNewSymbols)
   val  dataToUpdate =incrementalAggregatedDataExistingSymbol.union(incrementalAggregatedDataNewSymbols)

    hudiWriterConfig(dataToUpdate)
      .mode(Append).save(goldLayerPath)
  }


  def readData(path:String) = {
    spark.read
         .format(HUDI).load(path)
  }
}



object GoldLayerBuilder {

  def main(args: Array[String]): Unit = {
    val filePath = args(0)
    val spark = SparkFactory.getSparkSession()
    val configurationHelper = ConfigurationFactory.getConfiguration(filePath)

    val silverLayerPath = configurationHelper.getString("silverLayerPath")
    val goldLayerPath = configurationHelper.getString("goldLayerPath")
    val goldLayerBuilder = new GoldLayerBuilder(configurationHelper,spark)

    goldLayerBuilder.dumpExistingData(silverLayerPath,goldLayerPath)

    var goldLayerData =goldLayerBuilder.readData(goldLayerPath+"/*/*")

    println("Total number of records before incremental update "+ goldLayerData.count())
    goldLayerData.printSchema()
    goldLayerData.show(false)

    goldLayerBuilder.incrementalData(silverLayerPath,goldLayerPath)
    goldLayerData = goldLayerBuilder.readData(goldLayerPath+"/*/*")
    println("Total number of records after incremental update "+ goldLayerData.count())
    goldLayerData.show(false)
  }
}