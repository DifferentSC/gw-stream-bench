package edu.snu.splab.gwstreambench.nexmark;

import edu.snu.splab.gwstreambench.nexmark.source.NexmarkSourceGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public final class GeneratorMain {
    public static final void main(final String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final int eventsPerSecond = params.getInt("events_per_sec");
        final String kafkaBrokerAddress = params.get("kafka_broker_address");
        // final NexmarkSourceGenerator generator = new NexmarkSourceGenerator(eventsPerSecond);
        final NexmarkSourceGenerator generator = new NexmarkSourceGenerator(eventsPerSecond);
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokerAddress);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        final Producer<String, byte[]> kafkaProducer = new KafkaProducer<>(props);
        final Timer timer = new Timer();
        timer.scheduleAtFixedRate(new WordPublisherRunner(eventsPerSecond, generator, kafkaProducer),
                100L, 100L);
    }

    private static final class WordPublisherRunner extends TimerTask {
        private static final String TOPIC = "nexmarkinput";
        final int eventsToPublishInSingleRun;
        final NexmarkSourceGenerator generator;
        final Producer<String, byte[]> kafkaProducer;
        WordPublisherRunner(final int eventsPerSecond,
                            final NexmarkSourceGenerator generator,
                            final Producer<String, byte[]> kafkaProducer) {
            this.eventsToPublishInSingleRun = eventsPerSecond / 10;
            this.generator = generator;
            this.kafkaProducer = kafkaProducer;
        }

        @Override
        public void run() {
            System.out.println(System.currentTimeMillis());
            for (int i = 0; i < eventsToPublishInSingleRun; i++) {
                kafkaProducer.send(new ProducerRecord<>(TOPIC, generator.next()));
            }
        }
    }
}
