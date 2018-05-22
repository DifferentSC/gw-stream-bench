package edu.snu.splab.gwstreambench.query;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

/**
 * This class is for stateful class for simulating Samza VLDB experiment.
 */
public class StatefulCounter extends RichMapFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>> {

  private transient ValueState<Tuple3<Integer, Integer, String>> state;

  @Override
  public Tuple3<Integer, Integer, String> map(final Tuple2<Integer, String> in) throws Exception {
    final Tuple3<Integer, Integer, String> currentCount = state.value();
    currentCount.f1 += 1;
    currentCount.f2 = in.f1;
    state.update(currentCount);
    return currentCount;
  }

  @Override
  public void open(final Configuration config) {
    ValueStateDescriptor<Tuple3<Integer, Integer, String>> descriptor
        = new ValueStateDescriptor<> (
            "average",
        TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, String>>() {}),
        Tuple3.of(0, 0, ""));
  }
}
