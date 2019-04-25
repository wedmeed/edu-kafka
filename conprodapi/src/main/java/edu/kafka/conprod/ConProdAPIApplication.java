package edu.kafka.conprod;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConProdAPIApplication {

	private static Properties producerProps = new Properties();
	private static Properties consumerProps = new Properties();
	static{
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "apache-kafka:9092");
		producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "apache-kafka:9092");
		consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
		consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		consumerProps.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	}


	public static void main(String[] args) {

		//--------------------------------------------------------------------
        // 1. Create Producer
        Producer<String, String> producer = new KafkaProducer<>(producerProps);
        // 2. Send record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                "test-topic", "super-key", "super-value");
        producer.send(producerRecord);
        log.warn("---------- [{}] has been sent ----------", producerRecord);
        // 3. Close producer like a Master
        producer.close();

		//--------------------------------------------------------------------
		// 1. Create Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
		consumer.subscribe(Collections.singletonList("test-topic"));
		// 2. Start to consume records
		for (int i=0; i<3; i++) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			// 2a. Put your processing code here
			for (ConsumerRecord<String, String> record : records)
				log.warn("---------- Entry has been received. Offset = [{}], key = [{}], value = [{}] ----------",
						record.offset(), record.key(), record.value());
		}
		// 3. Close consumer like the God
		consumer.close();

	}

}
