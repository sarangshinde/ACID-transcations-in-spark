package hudi.basics

import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME_OPT_KEY, END_INSTANTTIME_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL, QUERY_TYPE_OPT_KEY}
import Constants._
object QueryData extends App{


  def pointInTimeQuery(): Unit =
  {
    // incrementally query data
    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime").collect()
      .map(k => k.getString(0)).take(50)
    println(" commits ")
    commits.foreach( (commit) => print(commit))

    val beginTimeN = "000" // Represents all commits > this time.
    val endTime = commits(commits.length - 2) // commit time we are interested in

    //incrementally query data
    val tripsPointInTimeDF = spark.read.format("hudi").
      option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME_OPT_KEY, beginTimeN).
      option(END_INSTANTTIME_OPT_KEY, endTime).
      load(BASEPATH)
    tripsPointInTimeDF.createOrReplaceTempView("hudi_trips_point_in_time")

    println("------Point in time query--------")
    spark.sql("select `_hoodie_commit_time`, driver,fare, begin_lon, begin_lat, ts from hudi_trips_point_in_time where fare > 20.0").show()

  }


  def incrementalQuery(): Unit =
  {
    println(" Incremental Query ")
    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime").map(k => k.getString(0)).take(50)
    val beginTime = commits(commits.length - 2) // commit time we are interested in

    // incrementally query data
    val tripsIncrementalDF = spark.read.format("hudi").
      option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
      load(BASEPATH)
    tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()

  }

  val spark = SparkFactory.getSparkSession

  val tripsSnapshotDF = spark.
    read.
    format("hudi").
    load(BASEPATH + "/*/*/*/*")
  //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
  tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

  spark.sql("select driver,fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()

  spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()

  println(" All data ")
  spark.sql("select * from hudi_trips_snapshot").show


   incrementalQuery()

  pointInTimeQuery()

}
