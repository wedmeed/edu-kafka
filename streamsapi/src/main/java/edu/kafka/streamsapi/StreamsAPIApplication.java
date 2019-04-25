package edu.kafka.streamsapi;

import edu.kafka.streamsapi.processor.MetadataProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

@Slf4j
public class StreamsAPIApplication {

	static Properties config = new Properties();
	static{
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsAPIApplication");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "apache-kafka:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {

		StreamsBuilder builder = new StreamsBuilder();

		setupStreaming(builder);

		Topology topology = builder.build();
		log.info(topology.describe().toString());

		KafkaStreams streaming = new KafkaStreams(topology, config);
		streaming.start();

		// Imagine a gracefull shutdown code here =)

	}

	static void setupStreaming(StreamsBuilder builder){
//		builder
//				.<String,String>stream("test-topic")
//				.filter((key, value) -> !key.equals(value))
//				.transformValues(MetadataProcessor::new)
//				.to("test-enriched-topic");

//		KStream<String,String>[] branches = builder
//				.<String,String>stream("test-topic")
//				.branch((key, value) -> !key.equals(value));
//		branches[0]
//				.transformValues(MetadataProcessor::new)
//				.merge(branches[0])
//				.to("test-enriched-topic");

//		builder
//				.<String,String>stream("test-topic")
//				.groupByKey()
//				.count()
//				.toStream()
//				.mapValues(String::valueOf)
//				.to("test-enriched-topic");

//		builder
//				.<String,String>stream("test-topic")
//				.groupByKey()
//				.count()
//				.toStream()
//				.mapValues(String::valueOf)
//				.to("test-enriched-topic");

//		KTable<String,String> table = builder
//				.<String,String>table("test-table-topic");
//		builder
//				.<String,String>stream("test-topic")
//				.leftJoin(table,(leftValue, rightValue)-> leftValue + " " + rightValue)
//				.to("test-table-topic");

		builder
				.<String,String>stream("test-topic")
				.groupByKey()
				.aggregate(
						String::new,
						(aggKey, newValue, aggValue) -> newValue + " " + aggValue)
				.toStream()
				.to("test-table-topic");
	}

}

