package edu.kafka.procapi.processors;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class MetadataProcessor implements Processor<String, String> {
    private ProcessorContext context;

    @Override
    //@SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // keep the processor context locally
        this.context = context;
    }

    @Override
    public void process(String key, String value) {
        // add header with tracingID
        this.context.headers().add("key", key.getBytes() );
        // forward to next stage
        this.context.forward(key,value + " but has header");
    }

    @Override
    public void close() {
        // close any resources managed by this processor
    }

}