package hudi.basics

import org.apache.hudi.QuickstartUtils.DataGenerator
import org.apache.spark.sql.SparkSession

object SparkFactory {

  def getSparkSession() :SparkSession = {
   SparkSession.builder()
    .appName("HudiDataGenerator")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
  }
}


object DataGeneratorFactory {
  val dataGen = new DataGenerator

  def getDataGenerator() ={
    dataGen
  }
}