package edu.kafka.streamsapi.processor;

import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class MetadataProcessor implements ValueTransformerWithKey<String, String, String> {
    private ProcessorContext context;

    @Override
    //@SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // keep the processor context locally
        this.context = context;
    }

    @Override
    public String transform(String key, String value) {
        // add header with tracingID
        this.context.headers().add("key", key.getBytes() );
        // forward to next stage
        return value + " but has header";
    }

    @Override
    public void close() {
        // close any resources managed by this processor
    }

}