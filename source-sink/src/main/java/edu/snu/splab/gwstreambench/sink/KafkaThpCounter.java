package edu.snu.splab.gwstreambench.sink;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Gyewon on 2018. 4. 16..
 */
public class KafkaThpCounter {

  public static final void main(final String[] args) throws Exception {

    final Options options = new Options();

    final Option kafkaBrokerAddressOpt = new Option("b", true, "The kafka broker address.");
    kafkaBrokerAddressOpt.setRequired(true);
    options.addOption(kafkaBrokerAddressOpt);

    final Option measuringTimeOpt = new Option("m", true, "The measuring time");
    measuringTimeOpt.setRequired(true);
    options.addOption(measuringTimeOpt);

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
    final long measuringTime = Long.valueOf(cmd.getOptionValue("m"));

    final Properties props = new Properties();
    props.put("bootstrap.servers", brokerAddress);
    props.put("group.id", "test");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
    final long startTime = System.currentTimeMillis();
    consumer.subscribe(Arrays.asList("result"));
    int count = 0;
    long endTime;
    do  {
      final ConsumerRecords<String, String> records = consumer.poll(100);
      count += records.count();
      endTime = System.currentTimeMillis();
    } while(endTime - startTime < measuringTime);
    final double thp = (double) count / ((endTime - startTime) / 1000.);
    final BufferedWriter writer = new BufferedWriter(new FileWriter("./thp.txt"));
    writer.write(String.valueOf(thp) + "\n");
    writer.close();
  }
}
