package hudi.basics

import java.util

import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.QuickstartUtils.{convertToStringList, getQuickstartWriteConfigs}
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.{DataFrame, Dataset}
import utilities.Constants._
import utilities.{DataGeneratorFactory, SparkFactory}

import scala.collection.JavaConversions._

object DataGenerator extends App {

  def hudiWriterConfig(df: DataFrame) = {
    df.write.format(HUDI).
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, HUDI_TABLENAME)
  }

  def inserts(): Unit = {
    import spark.implicits._
    val dataGen = DataGeneratorFactory.getDataGenerator()
    val inserts: util.List[String] = convertToStringList(dataGen.generateInserts(10))
    val data: Dataset[String] = spark.sparkContext.parallelize(inserts, 2).toDS()
    val df: DataFrame = spark.read.json(data)
    hudiWriterConfig(df).
      mode(Overwrite).
      save(HUDI_BASEPATH)
  }

  def updates(): Unit = {
    import spark.implicits._
    val dataGen = DataGeneratorFactory.getDataGenerator()
    val updates = convertToStringList(dataGen.generateUpdates(10))
    val data: Dataset[String] = spark.sparkContext.parallelize(updates, 2).toDS()
    val df = spark.read.json(data)
    hudiWriterConfig(df).
      mode(Append).
      save(HUDI_BASEPATH)
  }

  def query(query: String): Unit = {
    spark.sql(query).show(false)
  }


  val spark = SparkFactory.getSparkSession
  inserts()
  println("********************* Data after insert **********************")
  val tripsSnapshotDF = spark.read.format("hudi").load(HUDI_BASEPATH + "/*/*/*/*")
  //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
  tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
  query("select * from hudi_trips_snapshot")

  println("********************* Data after updates **********************")
  updates()
  val tripsSnapshotDFUpdates = spark.read.format("hudi").load(HUDI_BASEPATH + "/*/*/*/*")
  //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
  tripsSnapshotDFUpdates.createOrReplaceTempView("hudi_trips_snapshot")
  query("select * from hudi_trips_snapshot")

  println("********************* Incremental Pulls **********************")
  updates()
  val tripsSnapshotDFUpdates1 = spark.read.format("hudi").load(HUDI_BASEPATH + "/*/*/*/*")
  //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
  tripsSnapshotDFUpdates1.createOrReplaceTempView("hudi_trips_snapshot")

  import spark.implicits._

  val commits =
    spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime")
      .map(k => k.getString(0)).take(50)
  // commit time we are interested in
  val beginTime = commits(commits.length - 2)

  // incrementally query data
  val tripsIncrementalDF = spark.read.format("hudi")
    .option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL)
    .option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
    load(HUDI_BASEPATH)
  tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

  query("select * from hudi_trips_incremental")


}
