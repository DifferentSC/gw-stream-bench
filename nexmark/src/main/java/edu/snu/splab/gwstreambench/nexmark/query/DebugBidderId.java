package edu.snu.splab.gwstreambench.nexmark.query;

import edu.snu.splab.gwstreambench.nexmark.model.Bid;
import edu.snu.splab.gwstreambench.nexmark.model.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class DebugBidderId implements QueryBuilder {
    @Override
    public DataStream<String> build(DataStream<Event> in, StreamExecutionEnvironment env, ParameterTool params, Properties properties) throws Exception {
        return in.filter((FilterFunction<Event>) event -> event.eventType == Event.EventType.BID)
                .map((MapFunction<Event, Bid>) event -> event.bid)
                .map((MapFunction<Bid, Long>) bid -> bid.bidder)
                .map((MapFunction<Long, String>) bidder -> String.valueOf(bidder));
    }
}
