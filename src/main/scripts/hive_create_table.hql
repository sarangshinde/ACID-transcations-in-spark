CREATE EXTERNAL TABLE hudi_trips_cow
(`_hoodie_record_key` string,
`_hoodie_commit_time` string,
`_hoodie_commit_seqno` string,
`_hoodie_partition_path` string,
`_hoodie_file_name` string,
symbol string,
hour int,
total_volume double,
last_updated_at bigint)
PARTITIONED BY (`date` string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'hdfs:///user/hive/warehouse/gold_data_hudi';

ALTER TABLE `hudi_trips_cow` ADD IF NOT EXISTS PARTITION (`date`='20180831') LOCATION 'hdfs:///user/hive/warehouse/gold_data_hudi/20180831';
