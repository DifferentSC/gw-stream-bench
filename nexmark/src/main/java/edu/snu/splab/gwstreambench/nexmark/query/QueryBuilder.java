package edu.snu.splab.gwstreambench.nexmark.query;

import edu.snu.splab.gwstreambench.nexmark.model.Event;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public interface QueryBuilder {
    DataStream<String> build(DataStream<Event> in, StreamExecutionEnvironment env, ParameterTool params, Properties properties) throws Exception;
}
