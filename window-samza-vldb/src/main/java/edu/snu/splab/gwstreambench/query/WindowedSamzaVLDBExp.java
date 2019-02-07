
package edu.snu.splab.gwstreambench.query;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.OptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.StreamixStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
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

/**
 * This is a simulation of samza experiments on VLDB.
 */
public class WindowedSamzaVLDBExp {

  public static final void main(final String[] args) throws Exception {
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
    try {
      final ParameterTool params = ParameterTool.fromArgs(args);
      brokerAddress = params.get("broker_address", "");
      zookeeperAddress = params.get("zookeeper_address", "");
      dbPath = params.get("rocksdb_path", "");
      stateStorePath = params.get("state_store_path", "");
      blockCacheSize = params.getInt("block_cache_size");
      stateBackend = params.get("state_backend");
      textFilePath = params.get("text_file_path", "");
      // cacheSize = params.getInt("cache_size", 0);
      batchWriteSize = params.getInt("batch_write_size");
      batchReadSize = params.getInt("batch_read_size");
      writeBufferSize = params.getInt("write_buffer_size");
      fileNum = params.getInt("file_num");
      cachedRatio = params.getDouble("cached_ratio");
      windowSize = params.getInt("window_size", -1);
      windowInterval = params.getInt("window_interval", -1);
      sessionGap = params.getInt("session_gap", -1);
      queryType = params.get("query_type");
      tableFormat = params.get("table_format");
    } catch (final Exception e) {
      System.err.println("Missing configuration!" + e.toString());
      return;
    }

    // get the execution environment.
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

    final Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", brokerAddress);
    properties.setProperty("zookeeper.connect", zookeeperAddress);

    System.out.println("CheckpointingConfig: " + env.getCheckpointConfig().getCheckpointInterval());
    DataStream<String> count = null;
    if (queryType.equals("aggregate-count")) {
      final DataStream<String> text = env.readTextFile(textFilePath);
      System.out.println("Query type: Window with aggregate state");
      // parse the data, group it, window it, and aggregate the counts
      count = text
          .flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {
            public void flatMap(String value, Collector<Tuple2<Integer, String>> out) {
              String[] splitLine = value.split("\\s");
              out.collect(new Tuple2<>(Integer.valueOf(splitLine[0]), splitLine[1]));
            }
          })
          .keyBy(0)
          .countWindow(windowSize)
          .aggregate(new CountAggregate())
          .filter(x -> x.f0 == 1)
          .map(Tuple2::toString)
          .returns(String.class);
    } else if (queryType.equals("list-count")) {
      final DataStream<String> text = env.readTextFile(textFilePath);
      System.out.println("Query type: Count window with list state");
      // parse the data, group it, window it, and aggregate the counts
      count = text
          .flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {
            public void flatMap(String value, Collector<Tuple2<Integer, String>> out) {
              String[] splitLine = value.split("\\s");
              out.collect(new Tuple2<>(Integer.valueOf(splitLine[0]), splitLine[1]));
            }
          })
          .keyBy(0)
          .countWindow(windowSize)
          .process(new CountProcess())
          .filter(x -> x.f0 == 1)
          .map(x -> x.toString())
          .returns(String.class);
    } else if (queryType.equals("list-sliding-window")) {
      // get input data by connecting to the kafka server
      DataStream<String> text = env.addSource(
          new FlinkKafkaConsumer011<>("word", new SimpleStringSchema(), properties)
      );
      System.out.println("Query type: Sliding window with list state");
      // parse the data, group it, window it, and aggregate the counts
      count = text
          .flatMap(new FlatMapFunction<String, Tuple3<Integer, String, Long>>() {
            public void flatMap(String value, Collector<Tuple3<Integer, String, Long>> out) {
              String[] splitLine = value.split("\\s");
              out.collect(new Tuple3<>(Integer.valueOf(splitLine[0]), splitLine[1],
                  Long.valueOf(splitLine[2])));
            }
          })
          .keyBy(0)
          .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowInterval)))
          .process(new CountProcessWithLatency())
          // Leave only the latencies
          .map(x -> String.valueOf(System.currentTimeMillis() - x.f3))
          .returns(String.class);
    } else if (queryType.equals("session-window")) {
      // get input data by connecting to the kafka server
      DataStream<String> text = env.addSource(
          new FlinkKafkaConsumer011<>("word", new SimpleStringSchema(), properties)
      );
      System.out.println("Query type: Session window with list state");
      // parse the data, group it, window it, and aggregate the counts
      count = text
          .flatMap(new FlatMapFunction<String, Tuple3<Integer, String, Long>>() {
            public void flatMap(String value, Collector<Tuple3<Integer, String, Long>> out) {
              String[] splitLine = value.split("\\s");
              out.collect(new Tuple3<>(Integer.valueOf(splitLine[0]), splitLine[1],
                  Long.valueOf(splitLine[2])));
            }
          })
          .keyBy(0)
          .window(ProcessingTimeSessionWindows.withGap(Time.seconds(sessionGap)))
          .process(new CountProcessWithLatency())
          // Leave only the latencies
          .map(x -> String.valueOf(System.currentTimeMillis() - x.f3))
          .returns(String.class);
    } else {
      throw new IllegalArgumentException("Query should be one of aggregate-count, list-count, list-sliding-window," +
          " or session-window");
    }

    count.addSink(new FlinkKafkaProducer011<>(
        "result", new SimpleStringSchema(), properties));
    env.execute("Samza VLDB Window Experiment");
  }
}
