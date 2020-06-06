package iceberg.basics
import java.sql.Timestamp

import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.SparkSchemaUtil
import utilities.SparkFactory
import utilities.Constants._
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.catalog.TableIdentifier

object CreateTable extends App{
  val spark = SparkFactory.getSparkSession()
  import spark.implicits._
  val data = Seq((1, "pen",Timestamp.valueOf("2018-11-12 01:02:03.123456789")),
    (2, "pencil",Timestamp.valueOf("2018-11-12 02:02:03.123456789")),
    (3, "notebook",Timestamp.valueOf("2018-12-12 01:02:03.123456789")))
    .toDF("itemId", "itemName","itemPruchasedAtTime")

  def createTable(): Unit ={


    val schema = SparkSchemaUtil.convert(data.schema)
    println("schema is -> "+ schema)
    val spec = PartitionSpec.builderFor(schema)
      .hour("itemPruchasedAtTime")
      .build()
     val tables = new HadoopTables(spark.sessionState.newHadoopConf())
    println("schema for table is -> " + schema)
    val table = tables.create(schema, spec, ICEBERG_BASEPATH )



//    Not working error org/apache/hadoop/hive/metastore/api/UnknownDBException
//     val conf = spark.sparkContext.hadoopConfiguration
    //    conf.set("spark.hadoop.hive.metastore.uris","thrift://localhost:10000")
//    import org.apache.iceberg.hive.HiveCatalog
//    val catalog = new HiveCatalog(conf)
//    val name = TableIdentifier.of(ICEBERG_TABLENAME)
//    val table = catalog.createTable(name, schema, spec)



  }

  createTable

}
