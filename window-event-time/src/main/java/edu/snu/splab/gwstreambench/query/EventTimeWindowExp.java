package edu.snu.splab.gwstreambench.query;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class EventTimeWindowExp {

    public static final void main(final String[] args) throws Exception {

        //get 2 params as input
        final String brokerAddress;
        final String zookeeperAddress;
        try{
            final ParameterTool params = ParameterTool.fromArgs(args);
            brokerAddress = params.get("broker_address", "");
            zookeeperAddress = params.get("zookeeper_address", "");
        }catch(final Exception e){
            System.err.println("Missing configuration!" + e.toString());
            return;
        }

        // get the execution environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000);

        //set properties
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerAddress);
        properties.setProperty("zookeeper.connect", zookeeperAddress);

        //subscribe to "word" topic
        DataStream<String> text = env.addSource(
                new FlinkKafkaConsumer011<>("word", new SimpleStringSchema(), properties)
        );

        //parse "word" string to tuple3 containing (word, margin, timestamp)
        DataStream<Tuple3<Integer, String, Long>> tuples= text
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
                .keyBy(0);

        //generate watermark
        DataStream<Tuple3<Integer, String, Long>> withWatermarks = tuples
                .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator());


        DataStream<String> str = withWatermarks.flatMap(new FlatMapFunction<Tuple3<Integer, String, Long>, String>() {
            private String result;
            Long min_timeStamp=Long.MAX_VALUE, max_timeStamp= Long.valueOf(0);
            public void flatMap(Tuple3<Integer, String, Long> value, Collector<String> out) {
                if(min_timeStamp>value.f2) min_timeStamp=value.f2;
                if(max_timeStamp<value.f2) max_timeStamp=value.f2;

                String tmp;
                if(max_timeStamp - min_timeStamp <1000) tmp="  :)";
                else    tmp="  :(";
                result=value.f0.toString()+value.f1+value.f2.toString()+tmp;
                out.collect(result);
            }
        });
;
        str.addSink(new FlinkKafkaProducer011<String>(
            "result", new SimpleStringSchema(), properties));

        env.execute();
    }

    public static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<Tuple3<Integer, String, Long> > {

        private final long maxTimeLag = 1000; // 1 seconds

        @Override
        public long extractTimestamp(Tuple3<Integer, String, Long> element, long previousElementTimestamp) {
            return element.f2;
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current time minus the maximum time lag
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }
}
