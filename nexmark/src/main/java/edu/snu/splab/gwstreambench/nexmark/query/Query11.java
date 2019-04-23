package edu.snu.splab.gwstreambench.nexmark.query;

import edu.snu.splab.gwstreambench.nexmark.model.Event;
import edu.snu.splab.gwstreambench.nexmark.model.TimestampedEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.memory.ByteArrayDataInputView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class Query11 implements QueryBuilder {
    @Override
    public DataStream<String> build(final DataStream<Tuple2<Event, Long>> in, final StreamExecutionEnvironment env,
                                    final ParameterTool params, final Properties properties) throws Exception {
        return in.filter((FilterFunction<Tuple2<Event, Long>>) event -> event.f0.eventType == Event.EventType.BID)
                .map((MapFunction<Tuple2<Event, Long>, Tuple2<Long, Long>>) event -> new Tuple2<>(event.f0.bid.bidder, event.f1))
                .returns(new TypeHint<Tuple2<Long, Long>>() {})
                .keyBy(0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .process(new CountProcessWithLatency())
                .map((MapFunction<Tuple3<Long, Long, Long>, String>) sum -> String.valueOf(System.currentTimeMillis() - sum.f2))
                .returns(String.class);
    }
    public class CountProcessWithLatency
            extends ProcessWindowFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>,
            Tuple, TimeWindow> {

        final Tuple3<Long, Long, Long> result = new Tuple3<>();

        @Override
        public void process(Tuple key,
                            Context context,
                            Iterable<Tuple2<Long, Long>> data,
                            Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
            long count = 0;
            long maxTimestamp = -1L;

            for (final Tuple2<Long, Long> element: data) {
                count++;
                maxTimestamp = Math.max(maxTimestamp, element.f1);
            }
            long intKey = key.getField(0);
            result.setFields(intKey, count, maxTimestamp);
            collector.collect(result);
        }
    }
}
