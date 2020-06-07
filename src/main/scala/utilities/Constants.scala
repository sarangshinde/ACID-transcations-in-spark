package utilities

object Constants {

  val HUDI_TABLENAME = "hudi_trips_cow"
  val HUDI_BASEPATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/"+HUDI_TABLENAME

  val ICEBERG_TABLENAME = "iceberg_table"
  val ICEBERG_BASEPATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/"+ICEBERG_TABLENAME

  val DELTA_TABLENAME = "delta_lake_empdata"
  val DELTA_BASEPATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/"+DELTA_TABLENAME
  val DELTA_CHECKPOINT_PATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/checkpoint_"+DELTA_TABLENAME
  val DELTA_STREAMING_INPUT_PATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/streaming_input_"+DELTA_TABLENAME
  val DELTA_STREAMING_OUTPUT_PATH = "file:///Users/in-svsarang/Desktop/sarang/sparkwork/src/main/resources/streaming_output_"+DELTA_TABLENAME

  val DELTA = "delta"
  val APPEND = "append"
  val OVERWRITE = "overwrite"

}
