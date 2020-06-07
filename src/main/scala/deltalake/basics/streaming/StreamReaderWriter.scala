package deltalake.basics.streaming

import java.sql.Timestamp
import utilities.SparkFactory
import utilities.Constants._

object StreamReaderWriter extends App{

 val spark = SparkFactory.getSparkSession()

 val batch: Seq[Event] = Seq(
 Event(Timestamp.valueOf("2018-11-13 01:02:03.123456789"), 20181112,1,1, batch = 1),
 Event(Timestamp.valueOf("2018-11-13 02:02:03.123456789"), 20181112,2,2, batch = 1))

 GenerateData.generateData(batch)

 val readStreamData = spark.readStream.format(DELTA).load(DELTA_STREAMING_INPUT_PATH)

 val writeStreamData = readStreamData.writeStream.format(DELTA)
   .option("checkpointLocation", DELTA_CHECKPOINT_PATH)
   .start(DELTA_STREAMING_OUTPUT_PATH)
 writeStreamData.processAllAvailable()
 writeStreamData.stop()

 println("Latest Data")
 spark.read.format(DELTA).load(DELTA_STREAMING_OUTPUT_PATH).show(false)
}
