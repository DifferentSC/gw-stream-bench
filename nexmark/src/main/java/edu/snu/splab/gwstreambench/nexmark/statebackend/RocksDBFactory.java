package edu.snu.splab.gwstreambench.nexmark.statebackend;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.OptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.rocksdb.*;

public class RocksDBFactory implements StateBackendFactory {
    @Override
    public AbstractStateBackend get(ParameterTool params) throws Exception {
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
        return rocksDBStateBackend;
    }
}
