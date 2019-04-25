package edu.kafka.streamsapi;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class StreamsAPIApplicationTests {

	private TopologyTestDriver testDriver;
	private Serde<String> stringSerde = new Serdes.StringSerde();
	private ConsumerRecordFactory<String, String> factory
			= new ConsumerRecordFactory<>(stringSerde.serializer(), stringSerde.serializer());

	@Before
	public void setUp(){
		StreamsBuilder builder = new StreamsBuilder();
		StreamsAPIApplication.setupStreaming(builder);
		testDriver =
				new TopologyTestDriver(builder.build(), StreamsAPIApplication.config);
	}

    /**
     * The test is applicable for Case 5 and Case 6 only!
     * Disable it if you aren't studying for unit-testing
     */
	@Test
	public void checkSingleKeyAgg() {
		testDriver.pipeInput(factory.create("test-topic", "testKey", "testValue"));
		ProducerRecord<String, String> response
				= testDriver.readOutput("test-table-topic",
				stringSerde.deserializer(),
				stringSerde.deserializer());

		Assert.assertEquals(response.key(), "testKey");
		Assert.assertEquals(response.value(), "testValue ");

		testDriver.pipeInput(factory.create("test-topic", "testKey", "testValue2"));
		response = testDriver.readOutput("test-table-topic", stringSerde.deserializer(), stringSerde.deserializer());

		Assert.assertEquals(response.key(), "testKey");
		Assert.assertEquals(response.value(), "testValue2 testValue ");
	}

	@After
	public void tearDown(){
		try {
			testDriver.close();
		} catch (StreamsException ignore){
			// this is workaround for
			// https://issues.apache.org/jira/browse/KAFKA-6647
			// https://stackoverflow.com/questions/50602512/failed-to-delete-the-state-directory-in-ide-for-kafka-stream-application
			// https://stackoverflow.com/questions/52505306/kafka-streams-tests-do-not-correct-work-close
		}
	}
}
