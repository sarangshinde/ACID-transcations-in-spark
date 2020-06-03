package hudi.basics

import java.util

import org.apache.hudi.DataSourceWriteOptions.{OPERATION_OPT_KEY, PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.{Dataset, Row}
import scala.collection.JavaConversions._
import Constants._

object DeleteData extends App{

  val spark = SparkFactory.getSparkSession()
  val dataGen = DataGeneratorFactory.getDataGenerator()
  // run the same read query as above.
  val roBeforeDeleteViewDF = spark.
    read.
    format("hudi").
    load(BASEPATH + "/*/*/*/*")
  roBeforeDeleteViewDF.createOrReplaceTempView("hudi_trips_snapshot")
  // fetch should return (total - 2) records

  println("--Total Records before delete--")
  val count: Long = spark.sql("select uuid, partitionPath from hudi_trips_snapshot").count()
  println("Count -> "+ count )
  // fetch two records to be deleted
  val ds: Dataset[Row] = spark.sql("select uuid, partitionPath from hudi_trips_snapshot").limit(2)

  println("Records to be deleted for ")
  ds.foreach((data)=> println(data.getAs("uuid")))
  // issue deletes

  import spark.implicits._
  val deletes: util.List[String] = dataGen.generateDeletes(ds.collectAsList())
  val data = spark.sparkContext.parallelize(deletes, 2).toDS()
  val df = spark.read.json(data);
  df.write.format("hudi").
    options(getQuickstartWriteConfigs).
    option(OPERATION_OPT_KEY,"delete").
    option(PRECOMBINE_FIELD_OPT_KEY, "ts").
    option(RECORDKEY_FIELD_OPT_KEY, "uuid").
    option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
    option(TABLE_NAME, TABLENAME).
    mode(Append).
    save(BASEPATH)

  println("--Total Records after delete--")

  val roAfterDeleteViewDF = spark.
    read.
    format("hudi").
    load(BASEPATH + "/*/*/*/*")
  roAfterDeleteViewDF.createOrReplaceTempView("hudi_trips_snapshot_after_delete")

  val afterDeleteCount = spark.sql("select uuid, partitionPath from hudi_trips_snapshot_after_delete").count()
  println("Count -> "+ afterDeleteCount )


}
