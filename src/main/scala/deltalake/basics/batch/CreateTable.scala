package deltalake.basics.batch

import java.sql.Timestamp
import java.util.{Calendar, Date}

import io.delta.tables.DeltaTable
import org.joda.time.DateTime
import utilities.Constants.{DELTA, DELTA_BASEPATH}
import utilities.SparkFactory

object CreateTable extends App{

   def addData(path:String): Unit =
  {
    val spark = SparkFactory.getSparkSession()
    import spark.implicits._
    val data = Seq((1, "pen",new Timestamp(System.currentTimeMillis())),
      (2, "pencil",new Timestamp(System.currentTimeMillis())),
      (3, "notebook",new Timestamp(System.currentTimeMillis())))
      .toDF("itemId", "itemName","itemPruchasedAtTime")

    data.write.format(DELTA).mode("overwrite").save(path)

  }

  addData(DELTA_BASEPATH)
}
