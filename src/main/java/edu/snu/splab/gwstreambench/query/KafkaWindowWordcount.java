package edu.snu.splab.gwstreambench.query;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.OptionsFactory;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.util.Properties;

/**
 * The kafka window word count pipeline.
 */
public final class KafkaWindowWordcount {

  public static void main(String[] args) throws Exception {

    final String brokerAddress;
    final String zookeeperAddress;
    final String stateBackend;
    final String dbPath;
    final Integer windowSize;
    final Integer slidingInterval;
    try {
      final ParameterTool params = ParameterTool.fromArgs(args);
      brokerAddress = params.get("broker_address");
      zookeeperAddress = params.get("zookeeper_address");
      dbPath = params.get("rocksdb_path");
      stateBackend = params.get("state_backend");
      windowSize = params.getInt("window_size");
      slidingInterval = params.getInt("sliding_interval");
    } catch (final Exception e) {
      System.err.println("Missing configuration!");
      return;
    }

    // get the execution environment.
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Set the state backend.
    if (stateBackend.equals("rocksdb")) {
      final RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///tmp/");
      rocksDBStateBackend.setDbStoragePath(dbPath);
      rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
      rocksDBStateBackend.setOptions(new OptionsFactory() {
        @Override
        public DBOptions createDBOptions(DBOptions dbOptions) {
          return dbOptions;
        }
        @Override
        public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions columnFamilyOptions) {
          return columnFamilyOptions
              .setTableFormatConfig(new BlockBasedTableConfig().setNoBlockCache(true));
        }
      });
      env.setStateBackend(rocksDBStateBackend);
    } else if (stateBackend.equals("mem")) {
      env.setStateBackend(new MemoryStateBackend());
    } else {
      throw new IllegalArgumentException("The state backend should be one of rocksdb / mem");
    }

    final Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", brokerAddress);
    properties.setProperty("zookeeper.connect", zookeeperAddress);

    // get input data by connecting to the kafka server
    DataStream<String> text = env.
        addSource(new FlinkKafkaConsumer011<>("word", new SimpleStringSchema(), properties));

    // parse the data, group it, window it, and aggregate the counts
    DataStream<String> windowCounts = text
        .flatMap(new FlatMapFunction<String, WordWithCount>() {
          public void flatMap(String value, Collector<WordWithCount> out) {
            for (String word : value.split("\\s")) {
              out.collect(new WordWithCount(word, 1L));
            }
          }
        })
        .keyBy("word")
        .countWindow(windowSize, slidingInterval)
        .reduce(new ReduceFunction<WordWithCount>() {
          public WordWithCount reduce(WordWithCount a, WordWithCount b) {
            return new WordWithCount(a.word, a.count + b.count);
          }
        })
        .map(s -> s.toString());

    windowCounts.addSink(new FlinkKafkaProducer011<>("result", new SimpleStringSchema(), properties));

    env.execute("Socket Window WordCount");
  }
}
