package edu.snu.splab.gwstreambench.nexmark;

import edu.snu.splab.gwstreambench.nexmark.model.Event;
import edu.snu.splab.gwstreambench.nexmark.source.NexmarkSourceGenerator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.memory.ByteArrayDataOutputView;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public final class GeneratorMain {
    public static final void main(final String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final int eventsPerSecond = params.getInt("events_per_sec");
        final String kafkaBrokerAddress = params.get("kafka_broker_address");
        final TypeInformation<Event> eventTypeInfo = TypeExtractor.createTypeInfo(Event.class);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.getConfig().disableGenericTypes();
        final TypeSerializer<Event> serializer = eventTypeInfo.createSerializer(env.getConfig());
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
        timer.scheduleAtFixedRate(new WordPublisherRunner(eventsPerSecond, generator, serializer, kafkaProducer),
                100L, 100L);
    }

    private static final class WordPublisherRunner extends TimerTask {
        private static final String TOPIC = "nexmarkinput";
        final int eventsToPublishInSingleRun;
        final NexmarkSourceGenerator generator;
        final TypeSerializer<Event> serializer;
        final ByteArrayDataOutputView dataOutputView = new ByteArrayDataOutputView(1000);
        final Producer<String, byte[]> kafkaProducer;
        WordPublisherRunner(final int eventsPerSecond,
                            final NexmarkSourceGenerator generator,
                            final TypeSerializer<Event> serializer,
                            final Producer<String, byte[]> kafkaProducer) {
            this.eventsToPublishInSingleRun = eventsPerSecond / 10;
            this.generator = generator;
            this.serializer = serializer;
            this.kafkaProducer = kafkaProducer;
        }

        @Override
        public void run() {
            for (int i = 0; i < eventsToPublishInSingleRun; i++) {
                publish();
            }
        }

        private void publish() {
            try {
                final Event event = generator.next();
                if (event == null) {
                    return;
                }
                serializer.serialize(event, dataOutputView);
                kafkaProducer.send(new ProducerRecord<>(TOPIC, dataOutputView.toByteArray()));
                dataOutputView.reset();
            } catch (final IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }
}
