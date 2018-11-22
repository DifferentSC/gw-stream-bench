package edu.snu.splab.gwstreambench.sink;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by Gyewon on 2018. 4. 16..
 */
public class KafkaLatencyMeasure {

  public static final void main(final String[] args) throws Exception {

    final Options options = new Options();

    final Option kafkaBrokerAddressOpt = new Option("b", true, "The kafka broker address.");
    kafkaBrokerAddressOpt.setRequired(true);
    options.addOption(kafkaBrokerAddressOpt);

    final Option measuringTimeOpt = new Option("t", true, "The measuring time");
    measuringTimeOpt.setRequired(true);
    options.addOption(measuringTimeOpt);

    final Option deadlineLatencyOpt = new Option("d", true, "The deadline latency");
    deadlineLatencyOpt.setRequired(true);
    options.addOption(deadlineLatencyOpt);

    final Option loggingOpt = new Option("l", true, "Logging for debugging");
    loggingOpt.setRequired(false);
    options.addOption(loggingOpt);

    final CommandLineParser parser = new DefaultParser();
    final HelpFormatter formatter = new HelpFormatter();
    final CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (final ParseException e) {
      System.err.println(e);
      formatter.printHelp("mqtt-stream-source", options);
      System.exit(1);
      return;
    }
    final String brokerAddress = cmd.getOptionValue("b");
    final long measuringTime = Long.valueOf(cmd.getOptionValue("t"));
    final long deadlineLatency = Long.valueOf(cmd.getOptionValue("d"));
    final boolean loggingEnabled = Boolean.valueOf(cmd.getOptionValue("l"));

    final Properties props = new Properties();
    props.put("bootstrap.servers", brokerAddress);
    props.put("group.id", "test");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    final List<Long> latencies = new ArrayList<>();
    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    final long startTime = System.currentTimeMillis();
    consumer.subscribe(Arrays.asList("result"));
    long endTime;
    do {
      final ConsumerRecords<String, String> records = consumer.poll(100);
      for (final ConsumerRecord<String, String> record : records) {
        if (record.value() == null) {
          if (loggingEnabled) {
            System.out.println("Detected null record value! Record = " + record.toString());
          }
        } else {
          if (loggingEnabled) {
            System.out.println("Detected null record value! Record = " + record.toString());
          }
          latencies.add(Long.valueOf(record.value()));
        }
      }
      endTime = System.currentTimeMillis();
    } while(endTime - startTime < measuringTime);
    final BufferedWriter writer = new BufferedWriter(new FileWriter("./result.txt"));
    if (latencies.size() == 0) {
      writer.write("fail\n");
      writer.write("No event has been collected!");
      throw new RuntimeException("Fatal: No event hasn't been collected to sink yet!");
    }
    Collections.sort(latencies);
    // Currently, use P50 latency to determine whether it's successful or not.
    final double p50latency = latencies.get(latencies.size() / 2);
    if (p50latency <= deadlineLatency) {
      writer.write("success\n");
    } else {
      writer.write("fail\n");
    }
    writer.write("p50 latency = " + p50latency + "\n");
    writer.write("# of collected events = ");
    writer.close();
  }
}
