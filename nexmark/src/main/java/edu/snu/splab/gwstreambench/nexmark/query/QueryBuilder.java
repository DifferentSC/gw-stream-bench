package edu.snu.splab.gwstreambench.nexmark.query;

import edu.snu.splab.gwstreambench.nexmark.model.Event;
import edu.snu.splab.gwstreambench.nexmark.model.TimestampedEvent;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public interface QueryBuilder {
    DataStream<String> build(DataStream<Tuple2<Event, Long>> in, StreamExecutionEnvironment env, ParameterTool params, Properties properties) throws Exception;
}
