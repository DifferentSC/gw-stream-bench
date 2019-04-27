package edu.snu.splab.gwstreambench.query;

import org.apache.flink.contrib.streaming.state.StreamixStateBackend;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.OptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.StreamixStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.TableFormatConfig;



import java.util.Properties;


public class EventTimeWindowExp {

    static long maxTimeLag;
    public static final void main(final String[] args) throws Exception {

        //params in yaml file
        final String brokerAddress;
        final String zookeeperAddress;
        final String stateBackend;
        final String dbPath;
        final String stateStorePath;
        final Integer blockCacheSize;
        final String textFilePath;
        final Integer batchWriteSize;
        final Integer batchReadSize;
        final Integer writeBufferSize;
        final Integer fileNum;
        final Double cachedRatio;
        final Integer windowSize;
        final Integer windowInterval;
        final Integer sessionGap;
        final String queryType;
        final String tableFormat;
        final Integer parallelism;
        final Integer watermarkInterval;

        try{
            final ParameterTool params = ParameterTool.fromArgs(args);
            brokerAddress = params.get("broker_address", "");
            zookeeperAddress = params.get("zookeeper_address", "");
            queryType = params.get("query_type");
            stateBackend = params.get("state_backend");
            parallelism = params.getInt("parallelism");
            watermarkInterval = params.getInt("watermark_interval");
            maxTimeLag = params.getLong("max_timelag");

            //for session window
            sessionGap = params.getInt("session_gap", -1);

            //for rocksdb
            dbPath = params.get("rocksdb_path", "");
            blockCacheSize = params.getInt("block_cache_size", 0);
            writeBufferSize = params.getInt("write_buffer_size", 0);
            tableFormat = params.get("table_format");

            //for streamix
            stateStorePath = params.get("state_store_path", "");
            batchWriteSize = params.getInt("batch_write_size", 0);
            batchReadSize = params.getInt("batch_read_size", 0);
            cachedRatio = params.getDouble("cached_ratio", 0.0);
            fileNum = params.getInt("file_num", 1);


        }catch(final Exception e){
            System.err.println("Missing configuration!" + e.toString());
            return;
        }

        // get the execution environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();

        //Event-time specific
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(watermarkInterval);

        // Set the state backend.
        if (stateBackend.startsWith("rocksdb")) {
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
        } else if (stateBackend.equals("mem")) {
            env.setStateBackend(new FsStateBackend("file:///tmp"));
        } else if (stateBackend.equals("streamix")) {
            env.setStateBackend(new StreamixStateBackend(
                    stateStorePath,
                    batchWriteSize,
                    fileNum,
                    cachedRatio,
                    batchReadSize
            ));
        } else {
            throw new IllegalArgumentException("The state backend should be one of rocksdb / streamix / mem");
        }


        //set properties
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerAddress);
        properties.setProperty("zookeeper.connect", zookeeperAddress);

        DataStream<String> count = null;
        if(queryType.equals("session-window")){
            // get input data by connecting to the kafka server
            DataStream<String> text = env.addSource(
                    new FlinkKafkaConsumer011<>("word", new SimpleStringSchema(), properties)
            );
            System.out.println("Query type: Session window with list state");
            System.out.println(sessionGap);

            // parse the data, group it, window it, and aggregate the counts
            count = text
                    .flatMap(new FlatMapFunction<String, Tuple3< String, Long, Integer>>() {
                        private final Tuple3<String, Long, Integer> result = new Tuple3<>();
                        public void flatMap(String value, Collector<Tuple3< String, Long, Integer>> out) {
                            String[] splitLine = value.split("\\s");
                            result.f0 = splitLine[1];
                            result.f1 = Long.valueOf(splitLine[2]);
                            result.f2 = 0;
                            out.collect(result);
                        }
                    })
                    .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator())
                    .keyBy(0)
                    .window(EventTimeSessionWindows.withGap(Time.seconds(sessionGap)))
                    .sum(2)
                    .map(Tuple3::toString)
                    //.process(new CountProcessWithLatency())
                    // Leave only the latencies
                    //.map(x -> String.valueOf(System.currentTimeMillis() - x.f3))
                    .returns(String.class);
        } else {
            throw new IllegalArgumentException("Query should be one of aggregate-count, list-count, list-sliding-window," +
                    " or session-window");
        }

        count.addSink(new FlinkKafkaProducer011<>(
                "result", new SimpleStringSchema(), properties));

        env.execute("EventTime Session Window Experiment");

    }

    public static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer> > {
        //private final long maxTimeLag = 1000; // 1 seconds

        @Override
        public long extractTimestamp(Tuple3<String, Long, Integer> element, long previousElementTimestamp) {
            return element.f1;
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current time minus the maximum time lag
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }
}
