package utilities

import org.apache.hudi.QuickstartUtils.DataGenerator
import org.apache.spark.sql.SparkSession

object SparkFactory {

  def getSparkSession() :SparkSession = {
   val spark = SparkSession.builder()
    .appName("Transcations")
    .master("local[*]")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark

  }
}

object ConfigurationFactory{

  def getConfiguration(filename:String) ={

    new ConfigurationHelper(fileName=filename)

  }
}


object DataGeneratorFactory {
  val dataGen = new DataGenerator

  def getDataGenerator() ={
    dataGen
  }
}