package com.klarna.hiverunner;


import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.LocalModeHiveRunner;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;

@RunWith(StandaloneHiveRunner.class)
public class BasicDataTypesTestParquet {
	public static String stringFromParquetFile = "";

	private final String hdfsSourceFoo = "${hiveconf:hadoop.tmp.dir}/foo";
	private final String hdfsSourceBar = "${hiveconf:hadoop.tmp.dir}/bar";

	@HiveSetupScript
	String setup =

	"CREATE EXTERNAL TABLE `parquet_adx_bid_requests`("
			+ "`fileid` bigint COMMENT 'from deserializer',"
			+ "`version` bigint COMMENT 'from deserializer',"
			+ "`event_info` struct<timestamp:bigint,deprecated_region:bigint,adx_site_info:struct<id:bigint,domain_deprecated:string,content_categories:array<bigint>>,rtb_publisher_info:struct<id:bigint,name_deprecated:string>,data_center:bigint,app_info:struct<app_id:bigint,size:bigint,application:bigint,content_categories:array<bigint>>> COMMENT 'from deserializer',"
			+ "`ad_slots` bigint COMMENT 'from deserializer',"
			+ "`user_country` bigint COMMENT 'from deserializer',"
			+ "`eventid` string COMMENT 'from deserializer',"
			+ "`uuid` bigint COMMENT 'from deserializer',"
			+ "`size` bigint COMMENT 'from deserializer',"
			+ "`asset` bigint COMMENT 'from deserializer',"
			+ "`mobile_platform` bigint COMMENT 'from deserializer',"
			+ "`ip_data` struct<country:bigint,state:bigint,city:bigint,isp:bigint,dma:bigint,zip:bigint,connection_speed:bigint,user_domain:bigint,city_name:string,isp_name:string,gmt_offset:bigint,in_dst:boolean,time_zone:string,location:struct<latitude:bigint,longitude:bigint>> COMMENT 'from deserializer',"
			+ "`media_type` bigint COMMENT 'from deserializer',"
			+ "`deals` array<struct<id:bigint,price:double,pricingtype:string,price_usd:double>> COMMENT 'from deserializer',"
			+ "`private_exchange_deal_ids` array<string> COMMENT 'from deserializer',"
			+ "`bid_request_id` string COMMENT 'from deserializer',"
			+ "`ad_server_id` bigint COMMENT 'from deserializer',"
			+ "`sizes` array<bigint> COMMENT 'from deserializer',"
			+ "`dt` int," + "`hr` int)" + "ROW FORMAT SERDE"
			+ "'parquet.hive.serde.ParquetHiveSerDe'" + "STORED AS INPUTFORMAT"
			+ "'parquet.hive.DeprecatedParquetInputFormat'" + "OUTPUTFORMAT"
			+ "'parquet.hive.DeprecatedParquetOutputFormat'" + "LOCATION '"
			+ hdfsSourceFoo
			+ "' ;"
			+

			"CREATE EXTERNAL TABLE `parquet_adx_bid_requests1`("
			+ "`fileid` bigint COMMENT 'from deserializer',"
			+ "`version` bigint COMMENT 'from deserializer',"
			+ "`event_info` struct<timestamp:bigint,deprecated_region:bigint,adx_site_info:struct<id:bigint,domain_deprecated:string,content_categories:array<bigint>>,rtb_publisher_info:struct<id:bigint,name_deprecated:string>,data_center:bigint,app_info:struct<app_id:bigint,size:bigint,application:bigint,content_categories:array<bigint>>> COMMENT 'from deserializer',"
			+ "`ad_slots` bigint COMMENT 'from deserializer',"
			+ "`user_country` bigint COMMENT 'from deserializer',"
			+ "`eventid` string COMMENT 'from deserializer',"
			+ "`uuid` bigint COMMENT 'from deserializer',"
			+ "`size` bigint COMMENT 'from deserializer',"
			+ "`asset` bigint COMMENT 'from deserializer',"
			+ "`mobile_platform` bigint COMMENT 'from deserializer',"
			+ "`ip_data` struct<country:bigint,state:bigint,city:bigint,isp:bigint,dma:bigint,zip:bigint,connection_speed:bigint,user_domain:bigint,city_name:string,isp_name:string,gmt_offset:bigint,in_dst:boolean,time_zone:string,location:struct<latitude:bigint,longitude:bigint>> COMMENT 'from deserializer',"
			+ "`media_type` bigint COMMENT 'from deserializer',"
			+ "`deals` array<struct<id:bigint,price:double,pricingtype:string,price_usd:double>> COMMENT 'from deserializer',"
			+ "`private_exchange_deal_ids` array<string> COMMENT 'from deserializer',"
			+ "`bid_request_id` string COMMENT 'from deserializer',"
			+ "`ad_server_id` bigint COMMENT 'from deserializer',"
			+ "`sizes` array<bigint> COMMENT 'from deserializer',"
			+ "`dt` int,"
			+ "`hr` int)"
			+ "ROW FORMAT SERDE"
			+ "'org.openx.data.jsonserde.JsonSerDe'"
			+ "STORED AS INPUTFORMAT"
			+ "'org.apache.hadoop.mapred.TextInputFormat'"
			+ "OUTPUTFORMAT"
			+ "'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
			+ "LOCATION '"
			+ hdfsSourceBar + "' ;";
	
	//ROW FORMAT SERDE 
		//  'org.openx.data.jsonserde.JsonSerDe'
		//STORED AS INPUTFORMAT 
		//  'org.apache.hadoop.mapred.TextInputFormat' 
		//OUTPUTFORMAT 
		//  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'

	@HiveSQL(files = {}, autoStart = false)
	private HiveShell hiveShell;

	// Tests for Primitive Types
	@Test
	public void testBigInt() throws TException, IOException {
		hiveShell.addResource(hdfsSourceFoo + "/parquet.data", new File(
				ClassLoader.getSystemResource("parquetTest/parquet.txt")
						.getPath()));
		hiveShell.addResource(hdfsSourceBar + "/json.data", new File(
				ClassLoader.getSystemResource("parquetTest/json.data")
						.getPath()));
		System.out.println("Location"+ hdfsSourceBar);
		hiveShell.start();
		System.out.println("Both tables created ...");
		List<String> actual = hiveShell.executeQuery(String.format("select * from parquet_adx_bid_requests"));
		List<String> expected = hiveShell.executeQuery(String.format("select * from parquet_adx_bid_requests1"));
		//List<String> actual = hiveShell.executeQuery(String.format("select * from parquet_adx_bid_requests1"));
		System.out.println("PARQUET DATA :" + actual.get(0) +" JSON DATA :" + expected.get(0));
		System.out.println("Test completed");
	}

	// @Test
	public void testString() throws TException, IOException {
		// hiveShell.start();
		List<String> actual = hiveShell
				.executeQuery(String
						.format("select bid_request_id from parquet_adx_bid_requests;select ad_slots from parquet_adx_bid_requests;"));
		List<String> expected = Arrays
				.asList("0.0.1.77.74.200.250.198.71.24.99.239.173.224.75.49");

		Collections.sort(actual);
		Collections.sort(expected);

		Assert.assertEquals(expected, actual);
	}

	// Tests for Array of Primitives

	// Tests for Array of Structures of Primitives

	/*
	 * public void getArrayOfStructsFromJson() throws TException, IOException {
	 * // hiveShell.start(); //BaseTestsClassHelper helper = new
	 * BaseTestsClassHelper(); //String stringFromJsonTable =
	 * helper.testArrayOfStructs(); List<String> actual =
	 * hiveShell.executeQuery(String
	 * .format("select deals from parquet_adx_bid_requests where "));
	 * List<String> actual = hiveShell.executeQuery(String
	 * .format("select deals from parquet_adx_bid_requests where "));
	 * 
	 * String value=actual.get(0).toString(); String key="\"dummy\""; String
	 * json="{" +key+":" +value +"}"; System.out.println(json); mapFromJson =
	 * mapper.readValue(json.toString(), LinkedHashMap.class);
	 * 
	 * String finalval=mapper.writeValueAsString(mapFromJson); List<String>
	 * expected1 = Arrays
	 * .asList("{\"id\":1,\"price\":2},{\"id\":11,\"price\":0.22}");
	 * 
	 * Collections.sort(actual); //return mapFromJson;
	 * //Collections.sort(finalval);
	 * 
	 * //Assert.assertEquals(expected, actual); }
	 */

}
