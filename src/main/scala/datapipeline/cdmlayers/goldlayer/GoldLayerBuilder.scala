package datapipeline.cdmlayers.goldlayer

import hudi.basics.DataGenerator.hudiWriterConfig
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, max, sum}
import utilities.Constants.{DELTA, OVERWRITE}
import utilities.{ConfigurationFactory, ConfigurationHelper, SparkFactory}
import utilities.Constants._

class GoldLayerBuilder (configurationHelper:ConfigurationHelper,spark:SparkSession){

  import spark.implicits._

  def hudiWriterConfig(df:DataFrame) = {
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "last_updated_at").
      option(RECORDKEY_FIELD_OPT_KEY, "symbol").
     // option(PARTITIONPATH_FIELD_OPT_KEY, "date,hour").
      option(PARTITIONPATH_FIELD_OPT_KEY, "date").
      option(HIVE_TABLE_OPT_KEY, HUDI_TABLENAME).
      option(HIVE_PARTITION_FIELDS_OPT_KEY, "date").
      option(HIVE_PARTITION_FIELDS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName).
      option(TABLE_NAME, HUDI_TABLENAME)
  }

  def performAggregations(data:DataFrame):DataFrame={

    data.groupBy("symbol","date","hour")
      .agg(sum("volume").alias("total_volume"),
           max("time").as("last_updated_at"))
  }

  def dumpExistingData(silverLayerPath:String,goldLayerPath:String): Unit ={

    val silverLayerData = spark.read.format(DELTA).load(silverLayerPath)
    val aggregatedData = performAggregations(silverLayerData)
    hudiWriterConfig(aggregatedData)
      .partitionBy("date","hour")
      .mode(Append).save(goldLayerPath)
  }


  def incrementalData(silverLayerPath:String,goldLayerPath:String): Unit ={


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

    val goldLayerData = spark.
      read.
      format(HUDI).
      load(goldLayerPath + "/*/*")

    println("Total number of records "+ goldLayerData.count())
    goldLayerData.show(false)

  }
}