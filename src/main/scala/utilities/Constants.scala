package utilities

object Constants {

  val HUDI_TABLENAME = "hudi_trips_cow"
  val HUDI_COW_TABLENAME = "hudi_trips_cow"
  val HUDI_MOR_TABLENAME = "hudi_trips_mor"
  val HUDI_BASEPATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/"+HUDI_TABLENAME

  val ICEBERG_TABLENAME = "iceberg_table"
  val ICEBERG_BASEPATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/"+ICEBERG_TABLENAME

  val DELTA_TABLENAME = "delta_lake_empdata"
  val DELTA_BASEPATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/"+DELTA_TABLENAME

  val DELTA_HISTORY_VERSIONING_BASEPATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/history_versioning_"+DELTA_TABLENAME
  val DELTA_CHECKPOINT_PATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/checkpoint_"+DELTA_TABLENAME
  val DELTA_STREAMING_INPUT_PATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/streaming_input_"+DELTA_TABLENAME
  val DELTA_STREAMING_OUTPUT_PATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/streaming_output_"+DELTA_TABLENAME

  val SMALL_PARQUET_FILE_SPATH="file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/parquet_small_files"
  val COMPACTED_MOR_HUDI_FILES_PATH="file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/compacted_hudi_mor_files"
  val COMPACTED_COW_HUDI_FILES_PATH="file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/compacted_hudi_cow_files"
  val COMPACTED_DELTA_FILES_PATH="file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/compacted_delta_files"
  val DELTA = "delta"
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
