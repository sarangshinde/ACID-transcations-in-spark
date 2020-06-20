package datapipeline.consumer
import deltalake.basics.streaming.StreamReaderWriter.writeStreamData
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import utilities.{ConfigurationFactory, ConfigurationHelper, SparkFactory}
import utilities.Constants._
import utilities.ColumnConstants._

class KafkaConsumer(configurationHelper:ConfigurationHelper,spark:SparkSession){


  def consume()=
  {

    val kafkaBroker = configurationHelper.getString("kafkabroker")
    val rawData = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaBroker)
      .option("subscribe", "stock_ticks")
      .option("startingOffsets", "earliest")
      .load()

    import spark.implicits._

    val schema = StructType(Seq(StructField("close",DoubleType,true),
      StructField(DATE,StringType,true), StructField("day",StringType,true),
      StructField("high",DoubleType,true), StructField("key",StringType,true),
      StructField("low",DoubleType,true), StructField("month",StringType,true),
      StructField("open",DoubleType,true), StructField(ITEM,StringType,true),
      StructField("ts",StringType,true), StructField(QUANTITY,LongType,true),
      StructField("year",LongType,true)))

    val convertedJsonData = rawData.toDF().select(from_json($"value".cast("string"),schema).alias("stock_data"))
    convertedJsonData.selectExpr(s"regexp_replace(stock_data.${DATE},'/','') as ${DATE}",
      s"hour(cast(stock_data.ts as timestamp)) as ${HOUR}",
      s"cast(stock_data.ts as timestamp) as ${INVENTORY_TIME}",
      s"stock_data.${QUANTITY}", s"stock_data.${ITEM}").drop("open","close")
      .filter(col(ITEM).isin(MILK,BUTTER))



  }
  //for debuging
  def writeToConsole(data:DataFrame) = {
      data.writeStream.format("console").option("truncate","False").trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
  }

  //for debuging
  def writeToDeltaRawLayer(data:DataFrame) = {

    val rawDataPath = configurationHelper.getString("rawPath")
    val rawCheckPointPath = configurationHelper.getString("checkpointPath")


    data.writeStream.format(DELTA)
      .outputMode(APPEND)
      .partitionBy(DATE,HOUR)
      .option("checkpointLocation",rawCheckPointPath)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(rawDataPath)
  }


}

object KafkaConsumer {

  def main(args: Array[String]): Unit = {

    val filePath =  args(0)
    val spark = SparkFactory.getSparkSession()
    val configurationHelper = ConfigurationFactory.getConfiguration(filePath)
    val kafkaConsumer = new KafkaConsumer(configurationHelper,spark)
    val inputDataStream = kafkaConsumer.consume()

    //use for debuging
    // val streamingQuery = kafkaConsumer.writeToConsole(inputDataStream)

    val streamingQuery = kafkaConsumer.writeToDeltaRawLayer(inputDataStream)
    streamingQuery.awaitTermination()
  }


}

