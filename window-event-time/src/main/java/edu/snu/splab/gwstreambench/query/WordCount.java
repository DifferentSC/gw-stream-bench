package edu.snu.splab.gwstreambench.query;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class WordCount {

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

        //get parameters
        try {
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
            /*
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
            */

        } catch (final Exception e) {
            System.err.println("Missing configuration!" + e.toString());
            return;
        }


        //set env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();

        //Event-time specific
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(watermarkInterval);

        //set properties
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerAddress);
        properties.setProperty("zookeeper.connect", zookeeperAddress);

        // get input data by connecting to the kafka server
        DataStream<String> text = env.addSource(
                new FlinkKafkaConsumer011<>("word", new SimpleStringSchema(), properties)
        );

        //parse the data
        DataStream<String> count = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    private final Tuple2<String, Long> result = new Tuple2<>();

                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                        String[] splitLine = value.split("\\s");
                        result.f0 = splitLine[0];//word
                        result.f1 = Long.valueOf(splitLine[1]);//timestamp
                        out.collect(result);
                    }
                })
                .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator())
                .keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.seconds(sessionGap)))
                .sum(1)
                .map(Tuple2::toString)
                .returns(String.class);


        count.addSink(new FlinkKafkaProducer011<>(
                "result", new SimpleStringSchema(), properties));

        env.execute("EventTime Count Word");

    }

    public static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<Tuple2<String, Long> > {
        //private final long maxTimeLag = 1000; // 1 seconds

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            return element.f1;
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current time minus the maximum time lag
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }
}
