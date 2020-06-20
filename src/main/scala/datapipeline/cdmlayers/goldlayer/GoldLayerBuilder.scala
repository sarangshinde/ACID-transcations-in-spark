package datapipeline.cdmlayers.goldlayer

import hudi.basics.DataGenerator.hudiWriterConfig
import io.delta.tables.DeltaTable
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, max, row_number, sum}
import org.apache.spark.sql.types.StringType
import utilities.Constants.{DELTA, OVERWRITE}
import utilities.{ConfigurationFactory, ConfigurationHelper, SparkFactory}
import utilities.Constants._
import utilities.ColumnConstants._

class GoldLayerBuilder (configurationHelper:ConfigurationHelper,spark:SparkSession){

  import spark.implicits._

  def hudiWriterConfig(df:DataFrame) = {
    df.write.format(HUDI).
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "last_updated_at").
      option(RECORDKEY_FIELD_OPT_KEY, ITEM).
      option(PARTITIONPATH_FIELD_OPT_KEY, DATE).
      option(HIVE_TABLE_OPT_KEY, HUDI_TABLENAME).
      option(HIVE_PARTITION_FIELDS_OPT_KEY, DATE).
      option(HIVE_PARTITION_FIELDS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName).
      option(TABLE_NAME, HUDI_TABLENAME).
      option(OPERATION_OPT_KEY, UPSERT_OPERATION_OPT_VAL)
  }

  def performAggregationsUsingPartitonFor(incrementalSilverDataForExistingSymbols:DataFrame):DataFrame={

    val windowPartionByAppIdWithEventDescOrder: WindowSpec =
      Window.partitionBy(s"silverLayerData.${ITEM}",s"silverLayerData.${DATE}")
        .orderBy(col(INVENTORY_TIME).desc)

    val aggregatedData = incrementalSilverDataForExistingSymbols.withColumn(AGG_QUANTITY,
      sum(QUANTITY) over windowPartionByAppIdWithEventDescOrder )
      .withColumn("max_time",
        max(INVENTORY_TIME) over windowPartionByAppIdWithEventDescOrder )
      .withColumn("row_number",row_number over  windowPartionByAppIdWithEventDescOrder)
      .filter($"row_number"===1)

    val updateExistingAggregations= aggregatedData
      .withColumn(TOTAL_QUANTITY,
        col(s"goldLayerData.${TOTAL_QUANTITY}") + col(AGG_QUANTITY))
      .withColumn("last_updated_at",col("max_time"))
    updateExistingAggregations.select(s"silverLayerData.${ITEM}",s"silverLayerData.${DATE}",TOTAL_QUANTITY,"last_updated_at")

  }

  def performAggregations(data:DataFrame):DataFrame={

    data.groupBy(ITEM,DATE)
      .agg(sum(QUANTITY).alias(TOTAL_QUANTITY),
           max(INVENTORY_TIME).as("last_updated_at"))
      .withColumn(DATE,col(DATE).cast(StringType))
  }

  def dumpExistingData(silverLayerPath:String,goldLayerPath:String): Unit ={

    val silverLayerData = readAllSilverLayerData(silverLayerPath).filter(col(ITEM)===lit(MILK))
    val aggregatedData = performAggregations(silverLayerData)
    hudiWriterConfig(aggregatedData)
      .mode(Append).save(goldLayerPath)
  }


  def incrementalData(silverLayerPath:String,goldLayerPath:String): Unit ={

    val goldLayerData = readData(goldLayerPath+"/*/*").alias("goldLayerData")
    val silverLayerData = readLatestSilverLayerData(silverLayerPath)
      .select(ITEM,DATE,QUANTITY,INVENTORY_TIME).alias("silverLayerData")

    val incrementalSilverDataForExistingSymbols = goldLayerData.join(silverLayerData,
      ( col(s"goldLayerData.${ITEM}")===col(s"silverLayerData.${ITEM}")
        &&
        col(INVENTORY_TIME)>col("last_updated_at")
        )
    ).select("goldLayerData.last_updated_at",s"goldLayerData.${TOTAL_QUANTITY}","silverLayerData.*")

    val incrementalAggregatedDataExistingSymbol = performAggregationsUsingPartitonFor(incrementalSilverDataForExistingSymbols)

    val incrementalDataForNewSymbols = silverLayerData.join(goldLayerData.select(s"goldLayerData.${ITEM}"),
      col(s"goldLayerData.${ITEM}") === col(s"silverLayerData.${ITEM}"),"left_anti")
      .drop(s"goldLayerData.${ITEM}")

   val incrementalAggregatedDataNewSymbols  =  performAggregations(incrementalDataForNewSymbols)
   val  dataToUpdate =incrementalAggregatedDataExistingSymbol.union(incrementalAggregatedDataNewSymbols)

    hudiWriterConfig(dataToUpdate)
      .mode(Append).save(goldLayerPath)
  }

  def readLatestSilverLayerData(path:String) = {
    val deltaTable = DeltaTable.forPath(path)
    val latestVersion: Long = deltaTable.history().select("version")
      .collect().toSeq
      .map(row => row.getLong(0))
      .maxBy(_.longValue())

    spark.read
      .format(DELTA).option("versionAsOf",latestVersion).load(path)
  }

  def readAllSilverLayerData(path:String) = {
    spark.read
      .format(DELTA).load(path)
  }

  def readData(path:String) = {
    spark.read
         .format(HUDI).load(path)
  }
}



object GoldLayerBuilder {

  def main(args: Array[String]): Unit = {
    val filePath = args(0)
    val loadData= args(1) //dump existing data or load incrementally
    val spark = SparkFactory.getSparkSession()
    val configurationHelper = ConfigurationFactory.getConfiguration(filePath)

    val silverLayerPath = configurationHelper.getString("silverLayerPath")
    val goldLayerPath = configurationHelper.getString("goldLayerPath")
    val goldLayerBuilder = new GoldLayerBuilder(configurationHelper,spark)

    if(loadData.equalsIgnoreCase("incremental")) {
      goldLayerBuilder.incrementalData(silverLayerPath,goldLayerPath)
    }
    else{
      goldLayerBuilder.dumpExistingData(silverLayerPath, goldLayerPath)
    }
    val goldLayerData =goldLayerBuilder.readData(goldLayerPath+"/*/*")
        .select(ITEM,DATE,TOTAL_QUANTITY,"last_updated_at")
    println("Total number of records before incremental update "+ goldLayerData.count())
    goldLayerData.show(false)
  }
}