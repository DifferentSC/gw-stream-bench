package edu.snu.splab.gwstreambench.nexmark;

import edu.snu.splab.gwstreambench.nexmark.model.Event;
import edu.snu.splab.gwstreambench.nexmark.model.TimestampedEvent;
import edu.snu.splab.gwstreambench.nexmark.query.DebugBidderId;
import edu.snu.splab.gwstreambench.nexmark.query.Query11;
import edu.snu.splab.gwstreambench.nexmark.query.Query12;
import edu.snu.splab.gwstreambench.nexmark.query.QueryBuilder;
import edu.snu.splab.gwstreambench.nexmark.statebackend.StateBackendFactory;
import edu.snu.splab.gwstreambench.nexmark.statebackend.StreamixFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.OptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.memory.ByteArrayDataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.rocksdb.*;

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
        QUERY_BUILDERS.put("11", new Query11());
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
        if (stateBackend.equals("rocksdb")) {
            final String tableFormat = params.get("table_format");
            final String dbPath = params.get("rocksdb_path", "");
            final int writeBufferSize = params.getInt("write_buffer_size", 0);
            final int blockCacheSize = params.getInt("block_cache_size", 0);
            final RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///tmp/");
            rocksDBStateBackend.setDbStoragePath(dbPath);
            //rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
            //rocksDBStateBackend.setEnableStatistics(true);
            rocksDBStateBackend.setOptions(new OptionsFactory() {
                @Override
                public DBOptions createDBOptions(DBOptions dbOptions)
                {
                    return dbOptions
                            .setBytesPerSync(1024 * 1024);
                }
                @Override
                public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions columnFamilyOptions) {

                    final TableFormatConfig tableFormatConfig;
                    if (tableFormat.equals("block")) {
                        tableFormatConfig = new BlockBasedTableConfig();
                    } else if (tableFormat.equals("plain")) {
                        tableFormatConfig = new PlainTableConfig();
                    } else {
                        throw new IllegalArgumentException("RocksDB table format should be one of block or plain.");
                    }

                    return columnFamilyOptions
                            .setTableFormatConfig(new BlockBasedTableConfig()
                                    .setNoBlockCache(blockCacheSize == 0)
                                    .setBlockCacheSize(blockCacheSize * 1024 * 1024)
                                    .setBlockSize(16 * 1024)
                            )
                            .setWriteBufferSize(writeBufferSize * 1024 * 1024)
                            .setMemTableConfig(new SkipListMemTableConfig())
                            .setMaxWriteBufferNumber(1)
                            .setTargetFileSizeBase(128 * 1024 * 1024)
                            .setLevelZeroSlowdownWritesTrigger(40)
                            .setLevelZeroStopWritesTrigger(46)
                            .setBloomLocality(1)
                            .setCompressionType(CompressionType.NO_COMPRESSION)
                            .setTableFormatConfig(tableFormatConfig)
                            .useFixedLengthPrefixExtractor(16)
                            .setOptimizeFiltersForHits(false);
                    //optimizeForPointLookup(writeBufferSize * 1024 * 1024);
                }
            });
            env.setStateBackend(rocksDBStateBackend);

        } else if (!stateBackend.equals("default")) {
            final StateBackendFactory stateBackendFactory = STATE_BACKENDS.get(stateBackend);
            if (stateBackendFactory == null) {
                throw new UnsupportedOperationException(String.format("Unknown state backend: %s", stateBackend));
            }
            final AbstractStateBackend backend = stateBackendFactory.get(params);
            System.out.println(backend.getClass());
            env.setStateBackend(backend);
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
        final TypeInformation<TimestampedEvent> typeInformation = TypeExtractor.createTypeInfo(TimestampedEvent.class);
        final DataStream<TimestampedEvent> events = env.addSource(
                new FlinkKafkaConsumer011<>("nexmarkinput",
                        new TypeInformationSerializationSchema<>(typeInformation, env.getConfig()), properties));
        final TypeSerializer<Event>eventTypeSerializer = TypeExtractor.createTypeInfo(Event.class).createSerializer(env.getConfig());
        final DataStream<Tuple2<Event, Long>> tuples = events.map((MapFunction<TimestampedEvent, Tuple2<Event, Long>>) timestampedEvent -> new Tuple2<>(eventTypeSerializer.deserialize(new ByteArrayDataInputView(timestampedEvent.event)), timestampedEvent.systemTimeStamp));
        queryBuilder.build(tuples, env, params, properties)
                .addSink(new FlinkKafkaProducer011<>("result", new SimpleStringSchema(), properties));

        // execute the query
        env.execute(String.format("Nexmark %s on %s", queryName, stateBackend));
    }

}
