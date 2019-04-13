package edu.snu.splab.gwstreambench.nexmark.query;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * Reads from Kafka source and writes to Kafka source.
 */
public class Identity implements QueryBuilder {
    @Override
    public void build(final StreamExecutionEnvironment env, final ParameterTool params, final Properties properties) {

    }
}
