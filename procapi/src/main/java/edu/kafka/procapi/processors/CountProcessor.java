package edu.kafka.procapi.processors;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class CountProcessor implements Processor<String, String> {
    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // keep the processor context locally
        this.context = context;
        this.kvStore = (KeyValueStore) context.getStateStore("Test-Counts");
    }

    @Override
    public void process(String key, String value) {

        Long cnt = this.kvStore.get(key);
        if (cnt == null) cnt = 0L;

        cnt++;

        this.kvStore.put(key, cnt);

        this.context.forward(key,cnt.toString());
    }

    @Override
    public void close() {
        // close any resources managed by this processor
    }

}