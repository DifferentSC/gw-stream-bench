package edu.snu.splab.gwstreambench.query;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.FileStateBackend;
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

import java.util.*;

/**
 * The kafka window word count pipeline.
 */
public final class KafkaWindowWordcount {

  public static void main(String[] args) throws Exception {

    final String brokerAddress;
    final String zookeeperAddress;
    final String stateBackend;
    final String dbPath;
    final String stateStorePath;
    final Integer blockCacheSize;
    final Integer windowSize;
    final Integer slidingInterval;
    final Integer k;
    try {
      final ParameterTool params = ParameterTool.fromArgs(args);
      brokerAddress = params.get("broker_address");
      zookeeperAddress = params.get("zookeeper_address");
      dbPath = params.get("rocksdb_path", "");
      stateStorePath = params.get("stateStorePath", "");
      blockCacheSize = params.getInt("block_cache_size", 0);
      stateBackend = params.get("state_backend");
      windowSize = params.getInt("window_size");
      slidingInterval = params.getInt("sliding_interval");
      k = params.getInt("k");
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
          if (blockCacheSize == 0) {
            return columnFamilyOptions
                .setTableFormatConfig(new BlockBasedTableConfig().setNoBlockCache(true));
          } else {
            return columnFamilyOptions
                .setTableFormatConfig(new BlockBasedTableConfig()
                    .setNoBlockCache(false)
                    .setBlockCacheSize(blockCacheSize * 1024 * 1024));
          }
        }
      });
      env.setStateBackend(rocksDBStateBackend);
    } else if (stateBackend.equals("mem")) {
      env.setStateBackend(new MemoryStateBackend());
    } else if (stateBackend.equals("file")) {
      final FileStateBackend fileStateBackend = new FileStateBackend(stateStorePath);
      env.setStateBackend(fileStateBackend);
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
        .flatMap(new FlatMapFunction<String, String>() {
          public void flatMap(String value, Collector<String> out) {
            for (String word : value.split("\\s")) {
              out.collect(word);
            }
          }
        })
        .countWindowAll(windowSize, slidingInterval)
        .aggregate(
            new AggregateFunction<String, Map<String, Integer>, Map<String, Integer>>() {
              @Override
              public Map<String, Integer> createAccumulator() {
                return new HashMap<>();
              }

              @Override
              public Map<String, Integer> add(String s, Map<String, Integer> stringIntegerMap) {
                if (!stringIntegerMap.containsKey(s)) {
                  stringIntegerMap.put(s, 0);
                }
                stringIntegerMap.replace(s, stringIntegerMap.get(s) + 1);
                return stringIntegerMap;
              }

              @Override
              public Map<String, Integer> getResult(Map<String, Integer> stringIntegerMap) {
                final Map<String, Integer> result = new HashMap<>();
                final SortedSet<Map.Entry<String, Integer>> sortedSet = new TreeSet<>((x, y) -> x.getValue()
                    .compareTo(y.getValue()));
                sortedSet.addAll(stringIntegerMap.entrySet());
                int count = 0;
                while (!sortedSet.isEmpty() && count < k) {
                  final Map.Entry<String, Integer> entry = sortedSet.first();
                  result.put(entry.getKey(), entry.getValue());
                  sortedSet.remove(entry);
                  count++;
                }
                return result;
              }

              @Override
              public Map<String, Integer> merge(Map<String, Integer> map1, Map<String, Integer> map2) {
                for (Map.Entry<String, Integer> entry: map2.entrySet()) {
                  if (map1.containsKey(entry.getKey())) {
                    map1.replace(entry.getKey(), map1.get(entry.getKey()) + entry.getValue());
                  } else {
                    map1.put(entry.getKey(), entry.getValue());
                  }
                }
                return map1;
              }
            })
        .flatMap(new FlatMapFunction<Map<String, Integer>, String>() {
          @Override
          public void flatMap(Map<String, Integer> result, Collector<String> out) throws Exception {
            for (final Map.Entry<String, Integer> entry: result.entrySet()) {
              out.collect(entry.getKey());
            }
          }
        });

    windowCounts.addSink(new FlinkKafkaProducer011<>("result", new SimpleStringSchema(), properties));

    env.execute("Top-K Wordcount");
  }
}
