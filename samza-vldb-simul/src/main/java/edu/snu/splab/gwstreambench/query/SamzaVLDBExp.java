
package edu.snu.splab.gwstreambench.query;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.FileStateBackend;
import org.apache.flink.contrib.streaming.state.OptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.rocksdb.*;

import java.util.Properties;

/**
 * This is a simulation of samza experiments on VLDB.
 */
public class SamzaVLDBExp {

  public static final void main(final String[] args) throws Exception {
    final String brokerAddress;
    final String zookeeperAddress;
    final String stateBackend;
    final String dbPath;
    final String stateStorePath;
    final Integer blockCacheSize;
    final String textFilePath;
    final String cacheOption;
    final Integer cacheSize;
    final Integer batchWriteSize;
    final Integer writeBufferSize;
    try {
      final ParameterTool params = ParameterTool.fromArgs(args);
      brokerAddress = params.get("broker_address");
      zookeeperAddress = params.get("zookeeper_address");
      dbPath = params.get("rocksdb_path", "");
      stateStorePath = params.get("state_store_path", "");
      blockCacheSize = params.getInt("block_cache_size", 0);
      stateBackend = params.get("state_backend");
      textFilePath = params.get("text_file_path");
      cacheOption = params.get("cache_option", "NONE");
      System.out.println("Cache Option = " + cacheOption);
      cacheSize = params.getInt("cache_size", 0);
      batchWriteSize = params.getInt("batch_write_size", 0);
      writeBufferSize = params.getInt("write_buffer_size", 0);
    } catch (final Exception e) {
      System.err.println("Missing configuration!");
      return;
    }

    // get the execution environment.
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Set the state backend.
    if (stateBackend.startsWith("rocksdb")) {
      final RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///tmp/");
      rocksDBStateBackend.setDbStoragePath(dbPath);
      //rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
      rocksDBStateBackend.setCacheOption(cacheOption);
      rocksDBStateBackend.setCacheSize(cacheSize);
      rocksDBStateBackend.setBatchWriteSize(batchWriteSize);
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
          return columnFamilyOptions
              .setTableFormatConfig(new BlockBasedTableConfig()
                    .setNoBlockCache(blockCacheSize == 0)
                    .setBlockCacheSize(blockCacheSize * 1024 * 1024)
                    .setBlockSize(16 * 1024)
              )
              .setWriteBufferSize(writeBufferSize * 1024 * 1024)
              .setMemTableConfig(new SkipListMemTableConfig())
              .setMaxWriteBufferNumber(16)
              .setTargetFileSizeBase(128 * 1024 * 1024)
              .setLevelZeroSlowdownWritesTrigger(40)
              .setLevelZeroStopWritesTrigger(46)
              .setBloomLocality(1)
              .setCompressionType(CompressionType.NO_COMPRESSION)
              .setOptimizeFiltersForHits(false);
          }
      });
      env.setStateBackend(rocksDBStateBackend);
    } else if (stateBackend.equals("mem")) {
      env.setStateBackend(new MemoryStateBackend());
    } else if (stateBackend.equals("file")) {
      final FileStateBackend fileStateBackend = new FileStateBackend(stateStorePath, batchWriteSize);
      env.setStateBackend(fileStateBackend);
    } else {
      throw new IllegalArgumentException("The state backend should be one of rocksdb / file / mem");
    }

    final Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", brokerAddress);
    properties.setProperty("zookeeper.connect", zookeeperAddress);

    // get input data by connecting to the kafka server
    DataStream<String> text = env.readTextFile(textFilePath);

    System.out.println("CheckpointingConfig: " + env.getCheckpointConfig().getCheckpointInterval());

    // parse the data, group it, window it, and aggregate the counts
    DataStream<String> count = text
        .flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {
          public void flatMap(String value, Collector<Tuple2<Integer, String>> out) {
            String[] splitLine = value.split("\\s");
            out.collect(new Tuple2<>(Integer.valueOf(splitLine[0]), splitLine[1]));
          }
        })
        .keyBy(0)
        .map(new StatefulCounter())
        .filter(x -> x.f0.equals("1"))
        .map(x -> x.toString())
        .returns(String.class);

    count.addSink(new FlinkKafkaProducer011<>("result", new SimpleStringSchema(), properties));
    env.execute("Samza VLDB Simulation");
  }
}
