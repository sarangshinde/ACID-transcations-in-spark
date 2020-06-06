package hudi.basics

import java.util

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.QuickstartUtils.{convertToStringList, getQuickstartWriteConfigs}
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.JavaConversions._
import utilities.Constants._
import utilities.{DataGeneratorFactory, SparkFactory}

object DataGenerator extends App{

  def hudiWriterConfig(df:DataFrame) = {
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, HUDI_TABLENAME)
  }

  def inserts(): Unit ={
    import spark.implicits._
    val dataGen = DataGeneratorFactory.getDataGenerator()
    val inserts: util.List[String] = convertToStringList(dataGen.generateInserts(10))
    val data: Dataset[String] = spark.sparkContext.parallelize(inserts, 2).toDS()
    val df: DataFrame = spark.read.json(data)
    hudiWriterConfig(df).
      mode(Overwrite).
      save(HUDI_BASEPATH)
  }

  def updates(): Unit ={
    import spark.implicits._
    val dataGen = DataGeneratorFactory.getDataGenerator()
    val updates = convertToStringList(dataGen.generateUpdates(10))
    val data: Dataset[String] = spark.sparkContext.parallelize(updates, 2).toDS()
    val df = spark.read.json(data)
    hudiWriterConfig(df).
      mode(Append).
      save(HUDI_BASEPATH)
  }


  val spark = SparkFactory.getSparkSession
  inserts()
  Thread.sleep(5000)
  updates()




}
