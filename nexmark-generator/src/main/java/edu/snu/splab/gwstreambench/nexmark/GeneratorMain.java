package edu.snu.splab.gwstreambench.nexmark;

import edu.snu.splab.gwstreambench.nexmark.model.Event;
import edu.snu.splab.gwstreambench.nexmark.source.NexmarkSourceGenerator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.memory.ByteArrayDataOutputView;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public final class GeneratorMain {
    public static final void main(final String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final int eventsPerSecond = params.getInt("events_per_sec");
        final TypeInformation<Event> eventTypeInfo = TypeExtractor.createTypeInfo(Event.class);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.getConfig().disableGenericTypes();
        final TypeSerializer<Event> serializer = eventTypeInfo.createSerializer(env.getConfig());
        final NexmarkSourceGenerator generator = new NexmarkSourceGenerator(eventsPerSecond);
        final Timer timer = new Timer();
        timer.scheduleAtFixedRate(new WordPublisherRunner(eventsPerSecond, generator, serializer), 100L, 100L);
    }

    private static final class WordPublisherRunner extends TimerTask {
        final int eventsToPublishInSingleRun;
        final NexmarkSourceGenerator generator;
        final TypeSerializer<Event> serializer;
        final ByteArrayDataOutputView dataOutputView = new ByteArrayDataOutputView(1000);
        WordPublisherRunner(final int eventsPerSecond,
                            final NexmarkSourceGenerator generator,
                            final TypeSerializer<Event> serializer) {
            this.eventsToPublishInSingleRun = eventsPerSecond / 10;
            this.generator = generator;
            this.serializer = serializer;
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
                serializer.serialize(event, dataOutputView);
                System.out.println(DatatypeConverter.printHexBinary(dataOutputView.toByteArray()));
                dataOutputView.reset();
            } catch (final IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }
}
