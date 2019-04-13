package edu.snu.splab.gwstreambench.nexmark.query;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public interface QueryBuilder {
    void build(StreamExecutionEnvironment env, ParameterTool params, Properties properties) throws Exception;
}
