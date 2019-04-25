package edu.kafka.procapi;

import edu.kafka.procapi.processors.CountProcessor;
import edu.kafka.procapi.processors.MetadataProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

@Slf4j
public class ProcAPIApplication {

	private static Properties config = new Properties();
	static{
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ProcAPIApplication");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "apache-kafka:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");
		config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
	}

	public static void main(String[] args) {

		Topology topology = new Topology();

		topology
				.addSource("Source", "test-topic")
				.addProcessor("Process", CountProcessor::new, "Source")
				.addStateStore(
						Stores.keyValueStoreBuilder(
								Stores.persistentKeyValueStore("Test-Counts"),
								Serdes.String(),
								Serdes.Long()
						),
						"Process")
				.addSink("Sink", "test-enriched-topic", "Process");

		log.info(topology.describe().toString());

		KafkaStreams streaming = new KafkaStreams(topology, config);
		streaming.start();

		// Imagine a gracefull shutdown code here =)

	}

}


//		topology
//				.addSource("Source", "test-topic")
//				.addProcessor("Process", CountProcessor::new, "Source")
//				.addStateStore(
//						Stores.keyValueStoreBuilder(
//								Stores.persistentKeyValueStore("Test-Counts"),
//								Serdes.String(),
//								Serdes.Long()
//						),
//						"Process")
//				.addSink("Sink", "test-enriched-topic", "Process");