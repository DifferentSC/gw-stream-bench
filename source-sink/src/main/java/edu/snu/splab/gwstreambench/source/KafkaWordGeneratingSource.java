package edu.snu.splab.gwstreambench.source;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;

/**
 * The kafka word generating source
 */
public class KafkaWordGeneratingSource {

  public static final void main(final String[] args) {

    final Options options = new Options();

    final Option kafkaBrokerAddressOpt = new Option("b", true,"The kafka broker address.");
    kafkaBrokerAddressOpt.setRequired(true);
    options.addOption(kafkaBrokerAddressOpt);

    final Option eventRatePerSecondOption = new Option("r", true,"The event generation rate per second");
    eventRatePerSecondOption.setRequired(true);
    options.addOption(eventRatePerSecondOption);

    final Option marginOption = new Option("m", true, "The size of margin bytes");
    marginOption.setRequired(true);
    options.addOption(marginOption);

    final Option numThreadsOption = new Option("t", true,"The number of timer threads used for source generation");
    numThreadsOption.setRequired(true);
    options.addOption(numThreadsOption);

    final Option numKeysOption = new Option("k", true,"The number of keys");
    numKeysOption.setRequired(true);
    options.addOption(numKeysOption);

    final Option skewnessOption = new Option("s", true,"The zipfian skewness of key distribution");
    skewnessOption.setRequired(true);
    options.addOption(skewnessOption);

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

    final String kafkaBrokerAddress = cmd.getOptionValue("b");
    final int eventRatePerSecond = Integer.valueOf(cmd.getOptionValue("r"));
    final int numThreads = Integer.valueOf(cmd.getOptionValue("t"));
    final int numKeys = Integer.valueOf(cmd.getOptionValue("k"));
    final int marginSize = Integer.valueOf(cmd.getOptionValue("m"));
    final double skewness = Double.valueOf(cmd.getOptionValue("s"));

    final Random random = new Random();
    final List<String> marginList = new ArrayList<>();
    for (int i = 0; i < 10000; i ++) {
      final byte[] marginBytes = new byte[marginSize];
      for (int j = 0; j < marginSize; j++) {
        marginBytes[j] = (byte) (random.nextInt(26) + 'a');
      }
      marginList.add(new String(marginBytes));
    }

    final Properties props = new Properties();
    props.put("bootstrap.servers", kafkaBrokerAddress);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    final Producer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
    final WordGenerator wordGenerator = new UniformWordGenerator(numKeys);
    // Start timer threads.
    for (int i = 0; i < numThreads; i++) {
      final Timer timer = new Timer();
      timer.scheduleAtFixedRate(
          new WordPublishRunner(
              kafkaProducer,
              wordGenerator,
              "word",
              eventRatePerSecond / numThreads,
              marginList),
          1000, 1000);
    }
    // Wait for 24 hrs.
    try {
      Thread.sleep(86400 * 1000);
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
    System.exit(0);
  }
}
