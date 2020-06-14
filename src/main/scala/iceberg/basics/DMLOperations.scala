package iceberg.basics

import java.sql.Timestamp

import iceberg.basics.CreateTable.data
import utilities.Constants._
import utilities.SparkFactory

object DMLOperations extends App{

  val spark = SparkFactory.getSparkSession()
  import spark.implicits._
  def insert(): Unit ={

    val data = Seq((4, "pen",Timestamp.valueOf("2019-11-12 01:02:03.123456789")),
      (5, "pencil",Timestamp.valueOf("2019-11-12 02:02:03.123456789")),
      (6, "notebook",Timestamp.valueOf("2019-12-12 01:02:03.123456789")))
      .toDF("itemId", "itemName","itemPruchasedAtTime")

    data.write
      .format("iceberg")
      .mode("append")
      .save(ICEBERG_BASEPATH)

  }

  def appendToSamePartition(): Unit = {

    val data = Seq((4, "new_pen",Timestamp.valueOf("2019-11-12 01:02:03.123456789")))
      .toDF("itemId", "itemName","itemPruchasedAtTime")

    data.write
      .format(ICEBERG)
      .mode(APPEND)
      .save(ICEBERG_BASEPATH)

  }

  def updateWithOverwrite(): Unit = {

    val data = Seq((4, "new_pen_update",Timestamp.valueOf("2019-11-12 01:02:03.123456789")))
      .toDF("itemId", "itemName","itemPruchasedAtTime")

    data.write
      .format("iceberg")
      .mode("overwrite")
      .save(ICEBERG_BASEPATH)

  }

  val tableData = spark.read.format("iceberg").load(ICEBERG_BASEPATH)
  tableData.createOrReplaceTempView("iceberg_emp_table")


  def query(query:String): Unit ={
    spark.sql(query).show(false)
  }


  insert()
  query("select * from iceberg_emp_table")
  appendToSamePartition()
  query("select * from iceberg_emp_table")
  updateWithOverwrite()
 query("select * from iceberg_emp_table")


}
