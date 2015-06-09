USE ${hiveconf:my.schema};
    
  CREATE EXTERNAL TABLE `parquet_adx_bid_requests`(
  `fileid` bigint COMMENT 'from deserializer', 
  `version` bigint COMMENT 'from deserializer', 
  `event_info` struct<timestamp:bigint,deprecated_region:bigint,adx_site_info:struct<id:bigint,domain_deprecated:string,content_categories:array<bigint>>,rtb_publisher_info:struct<id:bigint,name_deprecated:string>,data_center:bigint,app_info:struct<app_id:bigint,size:bigint,application:bigint,content_categories:array<bigint>>> COMMENT 'from deserializer', 
  `ad_slots` bigint COMMENT 'from deserializer', 
  `user_country` bigint COMMENT 'from deserializer', 
  `eventid` string COMMENT 'from deserializer', 
  `uuid` bigint COMMENT 'from deserializer', 
  `size` bigint COMMENT 'from deserializer', 
  `asset` bigint COMMENT 'from deserializer', 
  `mobile_platform` bigint COMMENT 'from deserializer', 
  `ip_data` struct<country:bigint,state:bigint,city:bigint,isp:bigint,dma:bigint,zip:bigint,connection_speed:bigint,user_domain:bigint,city_name:string,isp_name:string,gmt_offset:bigint,in_dst:boolean,time_zone:string,location:struct<latitude:bigint,longitude:bigint>> COMMENT 'from deserializer', 
  `media_type` bigint COMMENT 'from deserializer', 
  `deals` array<struct<id:bigint,price:double,pricingtype:string,price_usd:double>> COMMENT 'from deserializer', 
  `private_exchange_deal_ids` array<string> COMMENT 'from deserializer', 
  `bid_request_id` string COMMENT 'from deserializer', 
  `ad_server_id` bigint COMMENT 'from deserializer', 
  `sizes` array<bigint> COMMENT 'from deserializer',
  `dt` int,
  `hr` int)
ROW FORMAT SERDE 
  'parquet.hive.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'parquet.hive.DeprecatedParquetInputFormat' 
OUTPUTFORMAT 
  'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '${hiveconf:MY.HDFS.DIR}/foo/';
  
