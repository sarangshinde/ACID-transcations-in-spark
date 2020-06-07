package deltalake.basics.batch

import java.sql.Timestamp

import utilities.Constants.{DELTA, DELTA_BASEPATH}
import utilities.SparkFactory

object CreateTable extends App{
  val spark = SparkFactory.getSparkSession()
  import spark.implicits._

  val data = Seq((1, "pen",Timestamp.valueOf("2018-11-12 01:02:03.123456789")),
    (2, "pencil",Timestamp.valueOf("2018-11-12 02:02:03.123456789")),
    (3, "notebook",Timestamp.valueOf("2018-12-12 01:02:03.123456789")))
    .toDF("itemId", "itemName","itemPruchasedAtTime")

  data.write.format(DELTA).mode("overwrite").save(DELTA_BASEPATH)

}
