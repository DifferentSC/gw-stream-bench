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

import org.apache.flink.runtime.state.memory.MemoryStateBackend;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
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
	env.setStateBackend(new MemoryStateBackend());

	//env.setParallelism(parallelism);
        //env.getConfig().enableObjectReuse();

        //Event-time specific
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(watermarkInterval);
/*
        if (stateBackend.equals("streamix")) {
            env.setStateBackend(new StreamixStateBackend(
                    stateStorePath,
                    batchWriteSize,
                    fileNum,
                    cachedRatio,
                    batchReadSize
            ));
        } else {
            throw new IllegalArgumentException("The state backend should be one of STREAMIX");
        }
*/
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
            System.out.println("\nQuery type: Session window with list state with session gap");
            System.out.println("session gap: "+sessionGap);
	    System.out.println("text: ");
	    text.print();

	    // parse the data, group it, window it, and aggregate the counts
            count = text
                    .flatMap(new FlatMapFunction<String, Tuple3<Integer, String, Long>>() {
                        private final Tuple3<Integer, String, Long> result = new Tuple3<>();
                        public void flatMap(String value, Collector<Tuple3<Integer, String, Long>> out) {
                            String[] splitLine = value.split("\\s");
                            result.f0 = Integer.valueOf(splitLine[0]);
                            result.f1 = splitLine[1];
                            result.f2 = Long.valueOf(splitLine[2]);
                            out.collect(result);
                        }
                    })
                    .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator())
                    .keyBy(0)
		    .window(EventTimeSessionWindows.withGap(Time.seconds(sessionGap)))
                    //.window(ProcessingTimeSessionWindows.withGap(Time.seconds(sessionGap)))
		    .process(new CountProcessWithLatency())
                    // Leave only the latencies
                    .map(x -> String.valueOf(System.currentTimeMillis() - x.f3))
                    .returns(String.class);
        } else {
            throw new IllegalArgumentException("Query should be session-window");
        }

	System.out.println("\nAfter query handle");
        count.addSink(new FlinkKafkaProducer011<>(
                "result", new SimpleStringSchema(), properties));

        env.execute("EventTime Session Window Experiment");

    }

    public static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<Tuple3<Integer, String, Long> > {
        //private final long maxTimeLag = 1000; // 1 seconds

        @Override
        public long extractTimestamp(Tuple3<Integer, String, Long> element, long previousElementTimestamp) {
		System.out.println("extractTimestamp");
       		return element.f2;
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current time minus the maximum time lag
            	System.out.println("getCurrentWatermark");
		return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }
}