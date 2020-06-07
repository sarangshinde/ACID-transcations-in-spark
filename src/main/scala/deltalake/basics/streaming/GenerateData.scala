package deltalake.basics.streaming

import utilities.SparkFactory
import java.sql.Timestamp
import scala.concurrent.duration._
import org.apache.spark.sql.execution.streaming.MemoryStream
import utilities.Constants._

case class Event(time: Timestamp,date:Long,hour:Int, value: Long, batch: Long) extends Serializable

object GenerateData{

  def generateData(batch: Seq[Event]): Unit ={

    val spark = SparkFactory.getSparkSession()
    import spark.implicits._
    implicit val sqlCtx = spark.sqlContext
    val events = MemoryStream[Event]
    val values = events.toDS
    val memoryQuery = values.writeStream.format("memory").queryName("memStream").start
    events.addData(batch)
    memoryQuery.processAllAvailable()
    memoryQuery.stop()

    val data = spark.table("memStream").as[Event]
    println("Data to be written")
    data.show()
    data.write.partitionBy("date","hour").format(DELTA).mode(APPEND).save(DELTA_STREAMING_INPUT_PATH)

  }


}
