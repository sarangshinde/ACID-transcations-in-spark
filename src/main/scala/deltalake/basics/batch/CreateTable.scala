package deltalake.basics.batch


import org.apache.log4j.{Level, Logger}
import utilities.Constants.{DELTA, DELTA_BASEPATH}
import utilities.SparkFactory
import java.nio.file.Paths


object CreateTable extends App {


  def addData(path: String): Unit = {

    val spark = SparkFactory.getSparkSession()

    import spark.implicits._
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val data = Seq((1, "Pen", 5),
      (2, "Pencil", 10),
      (3, "Notebook", 4))
      .toDF("ItemId", "ItemName", "NumberSold")
    println(Paths.get(".").toAbsolutePath)
    data.write.format(DELTA).mode("overwrite").save(DELTA_BASEPATH)


  }

  addData(DELTA_BASEPATH)
}
