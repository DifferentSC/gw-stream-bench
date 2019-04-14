package edu.snu.splab.gwstreambench.nexmark.query;

import edu.snu.splab.gwstreambench.nexmark.model.Bid;
import edu.snu.splab.gwstreambench.nexmark.model.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1)
                .map((MapFunction<Tuple2<Long, Long>, String>) sum -> String.valueOf(sum.f1));

    }
}
