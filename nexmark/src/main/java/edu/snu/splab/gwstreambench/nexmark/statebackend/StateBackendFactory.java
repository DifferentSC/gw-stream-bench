package edu.snu.splab.gwstreambench.nexmark.statebackend;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.AbstractStateBackend;

public interface StateBackendFactory {
    AbstractStateBackend get(ParameterTool params) throws Exception;
}
