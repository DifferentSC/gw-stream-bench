package edu.snu.splab.gwstreambench.sink;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * The kafka source which generates a fixed number of tuples.
 */
public class KafkaFixedNumTupleThpCounter {

  public static final void main(final String[] args) throws Exception {

    final Options options = new Options();

    final Option kafkaBrokerAddressOpt = new Option("b", true, "The kafka broker address.");
    kafkaBrokerAddressOpt.setRequired(true);
    options.addOption(kafkaBrokerAddressOpt);

    final Option tupleNumOpt = new Option("n", true, "The number of all tuples");
    tupleNumOpt.setRequired(true);
    options.addOption(tupleNumOpt);

    final CommandLineParser parser = new DefaultParser();
    final HelpFormatter formatter = new HelpFormatter();
    final CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (final ParseException e) {
      System.err.println(e);
      formatter.printHelp("kafka-thp-counter", options);
      System.exit(1);
      return;
    }
    final String brokerAddress = cmd.getOptionValue("b");
    final int tupleNum = Integer.valueOf(cmd.getOptionValue("n"));

    final Properties props = new Properties();
    props.put("bootstrap.servers", brokerAddress);
    props.put("group.id", "test");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("result"));
    int count;
    final long startTime = System.currentTimeMillis();
    do  {
      final ConsumerRecords<String, String> records = consumer.poll(1000);
      count = records.count();
    } while (count > 0);
    final double elapsedTime = (System.currentTimeMillis() - startTime) / 1000.;
    final double thp = (double) tupleNum / elapsedTime;
    System.out.println("Elasped time = " + elapsedTime);
    System.out.println("Throughput = " + thp);
    System.exit(0);
  }
}
