package utilities

import java.nio.file.Paths

object Constants {
  val CURRENT_DIRECTORY = Paths.get(".").toAbsolutePath
  val HUDI_TABLENAME = "hudi_trips_cow"

  val HUDI_COW_TABLENAME = "hudi_trips_cow"
  val HUDI_MOR_TABLENAME = "hudi_trips_mor"
  val HUDI_BASEPATH = s"file:///${CURRENT_DIRECTORY}/hudi_data/"+HUDI_TABLENAME

  val ICEBERG_TABLENAME = "iceberg_table"
  val ICEBERG_BASEPATH = s"file:///${CURRENT_DIRECTORY}/iceberg_data/"+ICEBERG_TABLENAME


  val DELTA_TABLENAME = "delta_lake_inventory"
  val DELTA_HISTORY_VERSIONING_BASEPATH = "file:///${CURRENT_DIRECTORY}/src/main/resources/history_versioning_"+DELTA_TABLENAME
  val DELTA_BASEPATH = s"file:///${CURRENT_DIRECTORY}/delta_data/"+DELTA_TABLENAME
  val DELTA_CHECKPOINT_PATH = s"file:///${CURRENT_DIRECTORY}/delta_data/checkpoint_"+DELTA_TABLENAME
  val DELTA_STREAMING_INPUT_PATH = s"file:///${CURRENT_DIRECTORY}/delta_data/streaming_input_"+DELTA_TABLENAME
  val DELTA_STREAMING_OUTPUT_PATH = s"file:///${CURRENT_DIRECTORY}/delta_data/streaming_output_"+DELTA_TABLENAME

  val SMALL_PARQUET_FILE_SPATH=s"file:///${CURRENT_DIRECTORY}/hudi_data/parquet_small_files"
  val COMPACTED_MOR_HUDI_FILES_PATH=s"file:///${CURRENT_DIRECTORY}/hudi_data/compacted_hudi_mor_files"
  val COMPACTED_MOR_INLINE_COMPACT_HUDI_FILES_PATH=s"file:///${CURRENT_DIRECTORY}/hudi_data/compacted_hudi_mor_inline_compact_files"
  val COMPACTED_COW_HUDI_FILES_PATH=s"file:///${CURRENT_DIRECTORY}/hudi_data/compacted_hudi_cow_files"
  val COMPACTED_DELTA_FILES_PATH=s"file:///${CURRENT_DIRECTORY}/hudi_data/compacted_delta_files"
  val DELTA = "delta"
  val AVRO = "avro"
  val HUDI = "hudi"
  val APPEND = "append"
  val OVERWRITE = "overwrite"
  val PARQUET = "parquet"
  val ICEBERG ="iceberg"
  val EMPTY=""
  val DEFAULT_CONFIGURATION = "default.conf"

}


object ColumnConstants {

  val ITEM = "item"
  val DATE = "date"
  val HOUR = "hour"
  val QUANTITY = "quantity"
  val INVENTORY_TIME = "inventory_time"
  val NEXT_QUANTITY = "next_quantity"
  val MILK = "MILK"
  val BUTTER="BUTTER"
  val TOTAL_QUANTITY = "total_quantity"
  val AGG_QUANTITY = "agg_quantity"
  val QUANTITY_DIFFERENCE = "quantity_difference"
  val QUANTITY_RISE_OR_DROP ="quantity_rise_or_drop"
}
