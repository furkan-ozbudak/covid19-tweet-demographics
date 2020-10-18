package sparksql;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
//import org.apache.hadoop.hive.serde2.*;
import org.apache.spark.api.java.function.Function;

public class Controller {

	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("furkanozbudak").setMaster(
				"local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.hadoopConfiguration().set(
				"mapreduce.input.fileinputformat.input.dir.recursive", "true");
		sc.hadoopConfiguration().set(
				"spark.hive.mapred.supports.subdirectories", "true");
		sc.hadoopConfiguration()
				.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive",
						"true");

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(
				sc.sc());

		sqlContext.setConf("hive.input.dir.recursive", "true");
		sqlContext.setConf("hive.mapred.supports.subdirectories", "true");
		sqlContext.setConf("hive.supports.subdirectories", "true");
		sqlContext.setConf("mapred.input.dir.recursive", "true");

		sqlContext.sql("DROP TABLE IF EXISTS twitter");

		sqlContext
				.sql("CREATE EXTERNAL TABLE twitter(id STRING, language STRING, state STRING, country STRING, followers INT)"
						+ "  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION 'hdfs://localhost:8020/user/cloudera/1-spark-sql-input'");

		Row[] results = sqlContext.sql("FROM twitter SELECT *").collect();
		for (Row row : results) {
			System.out.println(row);
		}

		sc.close();
	}
}

// sqlContext.sql("LOAD DATA INPATH '/user/cloudera/tweetinput' INTO TABLE tweeter");
// sqlContext.sql("CREATE TABLE test2 (id INT, name STRING )"
// +
// "  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION 'hdfs://localhost:8020/user/cloudera/2'");
// DataFrame df = sqlContext.sql("SELECT * FROM table");

// Map <String, String> configMap = new HashMap<String, String>();
// configMap.put("hive.input.dir.recursive", "true");
// configMap.put("hive.mapred.supports.subdirectories", "true");
// configMap.put("hive.supports.subdirectories", "true");
// configMap.put("mapred.input.dir.recursive", "true");

// sqlContext.setConf("hive.metastore.warehouse.dir", "");

// sqlContext.conf().setConf(props);
// set hive.input.dir.recursive=true;
// set hive.mapred.supports.subdirectories=true;
// set hive.supports.subdirectories=true;
// set mapred.input.dir.recursive=true;

// sqlContext.sql("hive.input.dir.recursive=true;");
// sqlContext.sql("hive.mapred.supports.subdirectories=true;");
// sqlContext.sql("hive.supports.subdirectories=true;");
// sqlContext.sql("mapred.input.dir.recursive=true;");

// List<String> recordList =
// sqlContext.sql("SELECT * FROM twitter6").javaRDD().map(new Function<Row,
// String>() {
// private static final long serialVersionUID = 1L;
//
// public String call(Row row) {
// return row.getString(0) + "," + row.getString(1) + "," + row.getString(2) +
// "," + row.getString(3) + "," + row.getString(4);
// }
// }).collect();
//
// for(String record : recordList) {
// System.out.println(record);
// }