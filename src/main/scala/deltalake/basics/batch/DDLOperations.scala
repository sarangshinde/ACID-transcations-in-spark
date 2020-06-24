package deltalake.basics.batch

import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import utilities.Constants.{APPEND, DELTA, DELTA_BASEPATH, OVERWRITE}
import utilities.SparkFactory

object DDLOperations extends App {

  val spark = SparkFactory.getSparkSession()
  //seperate out
  import spark.implicits._

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def writeToDeltaLake(data: DataFrame, mode: String): Unit = {
    data.write
      .format(DELTA)
      .mode(mode)
      .save(DELTA_BASEPATH)
  }

  def writeToDeltaLakeWithMergeSchema(data: DataFrame, mode: String): Unit = {
    data.write
      .format(DELTA)
      .mode(mode)
      .option("mergeSchema", "true")
      .save(DELTA_BASEPATH)
  }

  def insertWithIncorrectDataType(): Unit = {
    val data = Seq((4, "Ink Pen", "1"))
      .toDF("ItemId", "ItemName", "NumberSold")
    writeToDeltaLake(data, APPEND)
  }

  def insertWithIncorrectSchema(): Unit = {
    val data = Seq((4, "Ink Pen", 1))
      .toDF("ItemId", "ItemName", "Sold")
    writeToDeltaLake(data, APPEND)
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

  /* A new item as been added to product catalog and we get sales transaction for that product*/
  println("2. Trying to insert data with incorrect data type")
  insertWithIncorrectDataType()

  println("3. Trying to insert data with incorrect schema")
  insertWithIncorrectSchema()

  println("4. Trying to insert data with different schema and merge option")
  insertWithDifferentSchemaMergeOption()
  query("select * from inventory_temp_table order by ItemId")

  val newTableData = spark.read.format(DELTA).load(DELTA_BASEPATH)
  newTableData.createOrReplaceTempView("inventory_temp_table")
  query("select * from inventory_temp_table order by ItemId")

}
