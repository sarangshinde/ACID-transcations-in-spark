package deltalake.basics.batch

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import utilities.Constants.{APPEND, DELTA, DELTA_BASEPATH}
import utilities.SparkFactory

object DDLOperationsMergeTrue extends App {

  val spark = SparkFactory.getSparkSession()
  //seperate out
  import spark.implicits._

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def writeToDeltaLakeWithMergeSchema(data: DataFrame, mode: String): Unit = {
    data.write
      .format(DELTA)
      .mode(mode)
      .option("mergeSchema", "true")
      .save(DELTA_BASEPATH)
  }

  def insertWithDifferentSchemaMergeOption(): Unit = {
    val data = Seq((4, "Ink Pen", 1))
      .toDF("ItemId", "ItemName", "Sold")
    writeToDeltaLakeWithMergeSchema(data, APPEND)
  }

  def query(query: String): Unit = {
    spark.sql(query).show(false)
  }

  val tableData = spark.read.format(DELTA).load(DELTA_BASEPATH)
  tableData.createOrReplaceTempView("inventory_temp_table")

  println("1. Existing Data")
  query("select * from inventory_temp_table order by ItemId")

  println("2. Trying to insert data with different schema and merge option")
  insertWithDifferentSchemaMergeOption()
  query("select * from inventory_temp_table order by ItemId")

  val newTableData = spark.read.format(DELTA).load(DELTA_BASEPATH)
  newTableData.createOrReplaceTempView("inventory_temp_table")
  query("select * from inventory_temp_table order by ItemId")

}
