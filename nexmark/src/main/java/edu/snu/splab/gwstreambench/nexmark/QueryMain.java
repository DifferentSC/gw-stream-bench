package edu.snu.splab.gwstreambench.nexmark;

import edu.snu.splab.gwstreambench.nexmark.model.Event;
import edu.snu.splab.gwstreambench.nexmark.query.DebugBidderId;
import edu.snu.splab.gwstreambench.nexmark.query.Query11;
import edu.snu.splab.gwstreambench.nexmark.query.Query12;
import edu.snu.splab.gwstreambench.nexmark.query.QueryBuilder;
import edu.snu.splab.gwstreambench.nexmark.statebackend.StateBackendFactory;
import edu.snu.splab.gwstreambench.nexmark.statebackend.StreamixFactory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class QueryMain {
    private static final Map<String, StateBackendFactory> STATE_BACKENDS;
    private static final Map<String, QueryBuilder> QUERY_BUILDERS;

    static {
        STATE_BACKENDS = new HashMap<>();
        STATE_BACKENDS.put("streamix", new StreamixFactory());

        QUERY_BUILDERS = new HashMap<>();
        QUERY_BUILDERS.put("12", new Query12());
        QUERY_BUILDERS.put("12", new Query11());
        QUERY_BUILDERS.put("debug-bidder-id", new DebugBidderId());
    }

    public static final void main(final String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final int parallelism = params.getInt("parallelism");
        final String brokerAddress = params.get("broker_address", "");
        final String zookeeperAddress = params.get("zookeeper_address", "");
        final String stateBackend = params.get("state_backend");
        final String queryName = params.get("nexmark_query");

        // set up env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();
        // env.getConfig().disableGenericTypes();

        // set up state backend
        if (!stateBackend.equals("default")) {
            final StateBackendFactory stateBackendFactory = STATE_BACKENDS.get(stateBackend);
            if (stateBackendFactory == null) {
                throw new UnsupportedOperationException(String.format("Unknown state backend: %s", stateBackend));
            }
            env.setStateBackend(stateBackendFactory.get(params));
        }

        // prepare properties
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerAddress);
        properties.setProperty("zookeeper.connect", zookeeperAddress);

        // build query
        final QueryBuilder queryBuilder = QUERY_BUILDERS.get(queryName);
        if (queryBuilder == null) {
            throw new UnsupportedOperationException(String.format("Unknown query: %s", queryName));
        }
        final TypeInformation<Event> typeInformation = TypeExtractor.createTypeInfo(Event.class);
        final DataStream<Event> events = env.addSource(
                new FlinkKafkaConsumer011<>("nexmarkinput",
                        new TypeInformationSerializationSchema<>(typeInformation, env.getConfig()), properties));
        queryBuilder.build(events, env, params, properties)
                .addSink(new FlinkKafkaProducer011<>("result", new SimpleStringSchema(), properties));

        // execute the query
        env.execute(String.format("Nexmark %s on %s", queryName, stateBackend));
    }

}
