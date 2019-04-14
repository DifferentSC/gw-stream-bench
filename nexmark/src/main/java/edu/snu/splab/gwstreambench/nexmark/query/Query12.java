package edu.snu.splab.gwstreambench.nexmark.query;

import edu.snu.splab.gwstreambench.nexmark.model.Bid;
import edu.snu.splab.gwstreambench.nexmark.model.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
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
                .map((MapFunction<Event, Bid>) event -> event.bid)
                .map((MapFunction<Bid, Long>) bid -> bid.bidder)
                .map((MapFunction<Long, Tuple2<Long, Long>>) bidder -> new Tuple2<>(bidder, 1L))
                .returns(new TypeHint<Tuple2<Long, Long>>() {})
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
                .aggregate(new AggregateFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        return new Tuple2<>(0L, 0L);
                    }

                    @Override
                    public Tuple2<Long, Long> add(Tuple2<Long, Long> value, Tuple2<Long, Long> acc) {
                        return new Tuple2<>(value.f0, value.f1 + acc.f1);
                    }

                    @Override
                    public Tuple2<Long, Long> getResult(Tuple2<Long, Long> acc) {
                        return acc;
                    }

                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                        return new Tuple2<>(longLongTuple2.f0, longLongTuple2.f1 + acc1.f1);
                    }
                })
                .map((MapFunction<Tuple2<Long, Long>, String>) sum -> String.format("%d %d", sum.f0, sum.f1));

    }
}
