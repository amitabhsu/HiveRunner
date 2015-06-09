USE ${hiveconf:my.schema};
  
 CREATE EXTERNAL TABLE parquetSerdeTable 
 (foo bigint)
ROW FORMAT SERDE
  'parquet.hive.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT
  'parquet.hive.DeprecatedParquetOutputFormat'
  LOCATION '${hiveconf:MY.HDFS.DIR}/foo/';
