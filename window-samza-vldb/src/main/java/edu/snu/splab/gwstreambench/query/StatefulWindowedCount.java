package edu.snu.splab.gwstreambench.query;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

/**
 * This class implements stateful windowed counter manually.
 * This class does not save the count info for each key in external stores.
 */
public class StatefulWindowedCount extends RichMapFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>> {

  private transient AggregatingState<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>> state;

  private final int windowSize;

  public StatefulWindowedCount(int windowSize) {
    this.windowSize = windowSize;
  }

  @Override
  public Tuple3<Integer, Integer, String> map(
      Tuple2<Integer, String> in
  ) throws Exception {
    state.add(in);
    return state.get();
  }

  @Override
  public void open(final Configuration config) {
    AggregatingStateDescriptor<Tuple2<Integer, String>,
        Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>> descriptor =
        new AggregatingStateDescriptor<>(
            "count-window-simul",
            new CountWindowAggregate(windowSize),
            TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, String>>() {})
        );
    state = getRuntimeContext().getAggregatingState(descriptor);
  }

  private class CountWindowAggregate implements AggregateFunction<
      Tuple2<Integer, String>, Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>> {

    private final int windowSize;

    CountWindowAggregate(int windowSize) {
      this.windowSize = windowSize;
    }

    @Override
    public Tuple3<Integer, Integer, String> createAccumulator() {
      return Tuple3.of(0, 0, "");
    }

    @Override
    public Tuple3<Integer, Integer, String> add(
        Tuple2<Integer, String> in,
        Tuple3<Integer, Integer, String> acc) {
      if (acc.f1 == 0 || acc.f1 == windowSize) {
        return new Tuple3<>(in.f0, 1, in.f1);
      } else {
        return new Tuple3<>(in.f0, acc.f1 + 1, in.f1);
      }
    }

    @Override
    public Tuple3<Integer, Integer, String> getResult(Tuple3<Integer, Integer, String> acc) {
      return acc;
    }

    @Override
    public Tuple3<Integer, Integer, String> merge(
        Tuple3<Integer, Integer, String> acc1,
        Tuple3<Integer, Integer, String> acc2) {
      return new Tuple3<>(acc1.f0, acc1.f1 + acc2.f1, acc1.f2);
    }
  }
}