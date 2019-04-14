package edu.snu.splab.gwstreambench.nexmark;

import edu.snu.splab.gwstreambench.nexmark.model.Event;
import edu.snu.splab.gwstreambench.nexmark.source.NexmarkSourceGenerator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Timer;
import java.util.TimerTask;

public final class GeneratorMain {
    public static final void main(final String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final int eventsPerSecond = params.getInt("events_per_sec");
        final TypeInformation<Event> eventTypeInfo = TypeExtractor.createTypeInfo(Event.class);
        final NexmarkSourceGenerator generator = new NexmarkSourceGenerator(eventsPerSecond);
        final Timer timer = new Timer();
        timer.scheduleAtFixedRate(new WordPublisherRunner(eventsPerSecond), 100L, 100L);
    }

    private static final class WordPublisherRunner extends TimerTask {
        final int eventsToPublishInSingleRun;
        WordPublisherRunner(final int eventsPerSecond) {
            this.eventsToPublishInSingleRun = eventsPerSecond / 10;
        }

        @Override
        public void run() {

        }
    }
}
