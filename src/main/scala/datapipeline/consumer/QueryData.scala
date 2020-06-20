package datapipeline.consumer

import deltalake.basics.batch.DMLOperations.spark
import utilities.Constants.{DELTA, DELTA_BASEPATH}
import org.apache.spark.sql.functions.{col}
import utilities.{ConfigurationFactory, SparkFactory}
import utilities.ColumnConstants._

object QueryData extends App{


  val filePath =  args(0)
  val spark = SparkFactory.getSparkSession()
  val configurationHelper = ConfigurationFactory.getConfiguration(filePath)

  def queryRawData(): Unit =
  {
    val rawDataPath = configurationHelper.getString("rawPath")
    val tableData = spark.read.format(DELTA).load(rawDataPath)
    import spark.implicits._
    tableData.filter(col(ITEM).isin(MILK,BUTTER))
      .orderBy(ITEM,INVENTORY_TIME).show(100,false)
  }

  queryRawData()

}
