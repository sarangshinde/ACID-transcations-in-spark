package deltalake.basics.batch

import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import utilities.Constants.{APPEND, DELTA, DELTA_BASEPATH, OVERWRITE}
import utilities.SparkFactory

object DMLOperations extends App{

  val spark = SparkFactory.getSparkSession()
  import spark.implicits._
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def writeToDeltaLake(data:DataFrame, mode:String): Unit = {
    data.write
      .format(DELTA)
      .mode(mode)
      .save(DELTA_BASEPATH)
  }
  def insert(): Unit ={
    val data = Seq((4, "Ink Pen",1))
      .toDF("ItemId", "ItemName","NumberSold")
     writeToDeltaLake(data,APPEND)
  }

  def query(query:String): Unit ={
    spark.sql(query).show(false)
  }

  val tableData = spark.read.format(DELTA).load(DELTA_BASEPATH)
  tableData.createOrReplaceTempView("inventory_temp_table")

  println("Existing Data")
  query("select * from inventory_temp_table order by ItemId")

  /* A new item as been added to product catalog and we get sales transaction for that product*/
  insert()

  println("Data after new insert for Ink Pen")
  query("select * from inventory_temp_table order by ItemId")



  updateWithOverwrite()
  println("Data after insert same value in same partition using overwrite")
  query("select * from inventory_temp_table order by ItemId")

  //Conditional update without overwrite
  val deltaTable = DeltaTable.forPath(spark, DELTA_BASEPATH)


  /* You received the return request for a product and you would like to update the KPI */
  deltaTable.update(
    condition = expr("itemName == 'Pen'"),
    set = Map("NumberSold" -> expr("NumberSold - 1")))

  val updates = Seq((1, "Pen",7),
    (2, "Pencil",20),
    (5, "SketchPens",6))
    .toDF("ItemId", "ItemName","NumberSold")

  deltaTable.alias("originalTable")
    .merge(
      updates.as("updates"),
      "originalTable.ItemId = updates.ItemId")
    .whenMatched
    .updateExpr(
      Map("originalTable.ItemName" -> "updates.ItemName",
          "originalTable.NumberSold"-> "updates.NumberSold"))
    .whenNotMatched
    .insertExpr(
      Map(
        "originalTable.ItemId" -> "updates.ItemId",
        "originalTable.ItemName" -> "updates.ItemName",
        "originalTable.NumberSold" -> "updates.NumberSold"))
    .execute()

  println("Data after upserts ")
  query("select * from inventory_temp_table order by ItemId")

  println("Data after updating conditionally ")
  query("select * from inventory_temp_table order by ItemId")


  println("Data after delete conditionally ")

  deltaTable.delete(condition = expr("itemName == 'Ink Pen'"))
  query("select * from inventory_temp_table order by ItemId")

  def updateWithOverwrite(): Unit = {
    val data = Seq((1, "Pen",15),
      (2, "Pencil",20),
      (3, "Notebook",6))
      .toDF("ItemId", "ItemName","NumberSold")
    writeToDeltaLake(data,OVERWRITE)
  }
  updateWithOverwrite()

  println("Data after insert same value in same partition using overwrite")
  query("select * from inventory_temp_table order by ItemId" )

}
