package edu.snu.splab.gwstreambench.nexmark.statebackend;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.StreamixStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;

public final class StreamixFactory implements StateBackendFactory {
    @Override
    public AbstractStateBackend get(final ParameterTool params) throws Exception {
        final String stateStorePath = params.get("state_store_path", "");
        final int batchWriteSize = params.getInt("batch_write_size", 0);
        final int batchReadSize = params.getInt("batch_read_size", 0);
        final int fileNum = params.getInt("file_num", 1);
        final double cachedRatio = params.getDouble("cached_ratio", 0.0);
        return new StreamixStateBackend(
                stateStorePath,
                batchWriteSize,
                fileNum,
                cachedRatio,
                batchReadSize
        );
    }
}
