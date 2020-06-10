package datapipeline.consumer

import deltalake.basics.batch.DMLOperations.spark
import utilities.Constants.{DELTA, DELTA_BASEPATH}
import utilities.{ConfigurationFactory, SparkFactory}

object QueryData extends App{


  val filePath =  args(0)
  val spark = SparkFactory.getSparkSession()
  val configurationHelper = ConfigurationFactory.getConfiguration(filePath)

  def queryRawData(): Unit =
  {
    val rawDataPath = configurationHelper.getString("rawPath")
    val tableData = spark.read.format(DELTA).load(rawDataPath)
    println("Total number of records "+tableData.count)
    tableData.show(false)
  }

  queryRawData()

}
