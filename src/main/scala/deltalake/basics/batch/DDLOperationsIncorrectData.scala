package deltalake.basics.batch

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import utilities.Constants.{APPEND, DELTA, DELTA_BASEPATH}
import utilities.SparkFactory

object DDLOperationsIncorrectData extends App {

  val spark = SparkFactory.getSparkSession()

  import spark.implicits._

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def writeToDeltaLake(data: DataFrame, mode: String): Unit = {
    data.write
      .format(DELTA)
      .mode(mode)
      .save(DELTA_BASEPATH)
  }

  def insertWithIncorrectDataType(): Unit = {
    val data = Seq((4, "Ink Pen", "1"))
      .toDF("ItemId", "ItemName", "NumberSold")
    writeToDeltaLake(data, APPEND)
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

}
