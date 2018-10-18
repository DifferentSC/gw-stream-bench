package edu.snu.splab.gwstreambench.query;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * The count aggregate function using process window function.
 */
public class CountProcess extends ProcessWindowFunction<Tuple2<Integer, String>,
    Tuple3<Integer, Integer, String>, Tuple, GlobalWindow> {

  @Override
  public void process(Tuple key,
                      Context context,
                      Iterable<Tuple2<Integer, String>> data,
                      Collector<Tuple3<Integer, Integer, String>> collector) throws Exception {
    int count = 0;
    String margin = "";
    for (final Tuple2<Integer, String> element: data) {
      count++;
      margin = element.f1;
    }
    final int intKey = key.getField(0);
    collector.collect(new Tuple3<>(intKey, count, margin));
  }
}
