package edu.snu.splab.gwstreambench.nexmark.query;

import edu.snu.splab.gwstreambench.nexmark.model.Bid;
import edu.snu.splab.gwstreambench.nexmark.model.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Properties;

public class Query12 implements QueryBuilder {
    @Override
    public DataStream<String> build(final DataStream<Event> in, final StreamExecutionEnvironment env,
                                    final ParameterTool params, final Properties properties) {
        return in.filter((FilterFunction<Event>) event -> event.eventType == Event.EventType.BID)
                .map((MapFunction<Event, Tuple2<Long, Long>>) event -> new Tuple2<>(event.bid.bidder, event.systemTimeStamp))
                .returns(new TypeHint<Tuple2<Long, Long>>() {})
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
                .aggregate(new AggregateFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>>() {
                    /*
                    In: (bidder, timestamp)
                    Acc: (bidder, count, maxTimestamp)
                    Out: (bidder, count, maxTimestamp)
                     */
                    @Override
                    public Tuple3<Long, Long, Long> createAccumulator() {
                        return new Tuple3<>(0L, 0L, 0L);
                    }

                    @Override
                    public Tuple3<Long, Long, Long> add(Tuple2<Long, Long> value, Tuple3<Long, Long, Long> acc) {
                        return new Tuple3<>(value.f0, acc.f1 + 1, Math.max(value.f1, acc.f2));
                    }

                    @Override
                    public Tuple3<Long, Long, Long> getResult(Tuple3<Long, Long, Long> acc) {
                        return acc;
                    }

                    @Override
                    public Tuple3<Long, Long, Long> merge(Tuple3<Long, Long, Long> acc0, Tuple3<Long, Long, Long> acc1) {
                        return new Tuple3<>(acc0.f0, acc0.f1 + acc1.f1, Math.max(acc0.f2, acc1.f2));
                    }
                })
                .map((MapFunction<Tuple3<Long, Long, Long>, String>) sum -> String.valueOf(sum.f2))
                .returns(String.class);

    }
}
