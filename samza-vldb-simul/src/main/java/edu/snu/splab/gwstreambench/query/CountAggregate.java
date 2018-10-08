package edu.snu.splab.gwstreambench.query;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * The aggregate function.
 */
public final class CountAggregate
    implements AggregateFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> {

  @Override
  public Tuple2<Integer, String> createAccumulator() {
    return new Tuple2<>(0, "");
  }

  @Override
  public Tuple2<Integer, String> add(Tuple2<Integer, String> value, Tuple2<Integer, String> acc) {
    return new Tuple2<>(acc.f0 + 1, value.f1);
  }

  @Override
  public Tuple2<Integer, String> getResult(Tuple2<Integer, String> acc) {
    return acc;
  }

  @Override
  public Tuple2<Integer, String> merge(Tuple2<Integer, String> acc1, Tuple2<Integer, String> acc2) {
    return new Tuple2<>(acc1.f0 + acc2.f0, acc2.f1);
  }
}
