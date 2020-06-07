package deltalake.basics.batch

import java.sql.Timestamp

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import utilities.Constants.{APPEND, DELTA, DELTA_BASEPATH, OVERWRITE}
import utilities.SparkFactory

object DMLOperations extends App{

  val spark = SparkFactory.getSparkSession()
  import spark.implicits._


  def writeToDeltaLake(data:DataFrame, mode:String): Unit = {
    data.write
      .format(DELTA)
      .mode(mode)
      .save(DELTA_BASEPATH)
  }
  def insert(): Unit ={
    val data = Seq((4, "new_pen",Timestamp.valueOf("2019-11-12 01:02:03.123456789")))
      .toDF("itemId", "itemName","itemPruchasedAtTime")
     writeToDeltaLake(data,APPEND)
  }


  def updateWithOverwrite(): Unit = {
    val data = Seq((4, "new_pen_update",Timestamp.valueOf("2019-11-12 01:02:03.123456789")))
      .toDF("itemId", "itemName","itemPruchasedAtTime")
    writeToDeltaLake(data,OVERWRITE)

  }



  def query(query:String): Unit ={
    spark.sql(query).show(false)
  }


  val tableData = spark.read.format(DELTA).load(DELTA_BASEPATH)
  tableData.createOrReplaceTempView("delta_lake_emp_table")

  println("Existing Data")
  query("select * from delta_lake_emp_table")
  insert()

  println("Data after new insert")
  query("select * from delta_lake_emp_table")


  //Conditional update without overwrite
  val deltaTable = DeltaTable.forPath(spark, DELTA_BASEPATH)

  deltaTable.update(
    condition = expr("itemName == 'new_pen'"),
    set = Map("itemId" -> expr("itemId + 1")))

  println("Data after updateing conditionally ")
  query("select * from delta_lake_emp_table")

  // Delete conditionally
  println("Data after delete conditionally ")
 // deltaTable.delete(condition = expr("itemName == 'new_pen'"))
  query("select * from delta_lake_emp_table")


  updateWithOverwrite()
  println("Data after insert same value in same partition using overwrite")
  query("select * from delta_lake_emp_table")
}
