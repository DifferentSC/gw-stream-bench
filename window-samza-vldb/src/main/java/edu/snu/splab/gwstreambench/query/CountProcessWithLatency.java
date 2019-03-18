package edu.snu.splab.gwstreambench.query;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The process function with list states.
 */
public class CountProcessWithLatency
    extends ProcessWindowFunction<Tuple3<Integer, String, Long>, Tuple4<Integer, Integer, String, Long>,
    Tuple, TimeWindow>  {

  final Tuple4<Integer, Integer, String, Long> result = new Tuple4<>();

  @Override
  public void process(Tuple key,
                      Context context,
                      Iterable<Tuple3<Integer, String, Long>> data,
                      Collector<Tuple4<Integer, Integer, String, Long>> collector) throws Exception {
    int count = 0;
    long maxTimestamp = -1L;
    String margin = "";

    for (final Tuple3<Integer, String, Long> element: data) {
      count++;
      if (element.f2 > maxTimestamp) {
        maxTimestamp = element.f2;
        margin = element.f1;
      }
    }
    int intKey = key.getField(0);
    result.setFields(intKey, count, margin, maxTimestamp);
    collector.collect(result);
  }
}
