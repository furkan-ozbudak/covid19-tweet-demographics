package sparkstreaming;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;

import scala.Tuple2;

public class Controller {

	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"kafkastream");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(15));

		Map<String, Integer> topicPartitionMap = new HashMap<>();
		topicPartitionMap.put("covid-tweets", 1);

		JavaPairDStream<String, String> s = KafkaUtils.createStream(jssc,
				"localhost:2181", "test", topicPartitionMap);
		s.map(x -> x._2).dstream()
				.saveAsTextFiles("1-spark-streaming-output/record", null);

		jssc.start();
		jssc.awaitTermination();
	}
}

// while (true) {
// ConsumerRecords<String, String> records = consumer.poll(5);
// for (ConsumerRecord<String, String> record : records) {
// System.out.printf("offset = %d, key = %s, value = %s",
// record.offset(), record.key(), record.value());

// stream.saveAsHadoopFiles("file:///furkan", "txt");
// stream.dstream().saveAsTextFiles("furkan", "txt");
// stream.foreachRDD(r -> {
// r.saveAsTextFile("streamOutput");
// });
// jssc,
// LocationStrategies.PreferConsistent(),
// ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
// );
// }
// }
// stream.saveAsHadoopFiles("kafkaConsumerOutput", "txt");

// stream.foreachRDD(r -> {
// r.saveAsTextFile("furkan");
// });
// stream.dstream().saveAsTextFiles("furkan", "txt");
// stream.foreachRDD(r -> {
// r.saveAsTextFile("furkan/record" + new Date().getTime());
// });

// stream.map(x -> new Tuple2(x._1, x._2));
// });
// JavaDStream<String> mappedStream = mappedStream.map(x -> x.charAt(1))

// JavaPairDStream<String, String> filtered = s.filter(
// new Function<Tuple2<String, String>, Boolean>() {
// private static final long serialVersionUID = 1L;
// public Boolean call(Tuple2<String, String> value) {
// return value._2.equals("furkan");
// }
// }
// );

// JavaPairDStream<String, String> mapped = filtered.ma

// JavaDStream<String> lines = stream.map((Function<ConsumerRecord<String,
// String>, String>) kafkaRecord -> kafkaRecord.value());
// JavaDStream<String> chars = lines.map(x -> x.charAt(0));

// Stream<String> s = stream.mapValues(x -> x.charAt(0);)
// stream.foreachRDD(rdd -> {
// rdd.ma
// });

// // Create a local StreamingContext with two working thread and batch
// interval of 1 second
// SparkConf conf = new
// SparkConf().setMaster("local[2]").setAppName("kafkastream");
// JavaStreamingContext jssc = new JavaStreamingContext(conf,
// Durations.seconds(600));
//
// Map<String, Object> kafkaParams = new HashMap<>();
//
// kafkaParams.put("bootstrap.servers", "localhost:9092");
// kafkaParams.put("key.deserializer", StringDeserializer.class);
// kafkaParams.put("value.deserializer", StringDeserializer.class);
// kafkaParams.put("group.id",
// "use_a_separate_group_id_for_each_stream");
// kafkaParams.put("auto.offset.reset", "latest");
// kafkaParams.put("enable.auto.commit", false);
//
// Collection<String> topics = Arrays.asList("cs523");

// JavaInputDStream<ConsumerRecord<String, String>> stream =
// KafkaUtils.createDirectStream(
// jssc,
// LocationStrategies.PreferConsistent(),
// ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
// );

// JavaPairDStream<String, String> data = stream.mapToPair(record -> new
// Tuple2<>(record.key(), record.value()));
// data.saveAsHadoopFiles("kafkaConsumerOutput", "txt");
// System.out.println(data);
// jssc.start(); // Start the computation
// jssc.awaitTermination(); // Wait for the computation to terminate

// KafkaUtils.createStream(arg0, arg1, arg2, arg3)

// KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
// consumer.subscribe(Arrays.asList("covid-tweets"));

// consumer.

// Properties props = new Properties();
// props.put("bootstrap.servers", "localhost:9092");
// props.put("group.id", "test");
// props.put("enable.auto.commit", "true");
// props.put("auto.commit.interval.ms", "30000");
// props.put("session.timeout.ms", "30000");
// props.put("key.deserializer",
// "org.apache.kafka.common.serialization.StringDeserializer");
// props.put("value.deserializer",
// "org.apache.kafka.common.serialization.StringDeserializer");

// s.dstream().saveAsTextFiles("1/record", null);