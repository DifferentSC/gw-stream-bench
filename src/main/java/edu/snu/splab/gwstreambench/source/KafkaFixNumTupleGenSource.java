package edu.snu.splab.gwstreambench.source;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * The kafka word generating source
 */
public class KafkaFixNumTupleGenSource {

  public static final void main(final String[] args) {

    final Options options = new Options();

    final Option kafkaBrokerAddressOpt = new Option("b", true,"The kafka broker address.");
    kafkaBrokerAddressOpt.setRequired(true);
    options.addOption(kafkaBrokerAddressOpt);

    final Option tupleNumOption = new Option("n", true,"The number of all tuples");
    tupleNumOption.setRequired(true);
    options.addOption(tupleNumOption);

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
      formatter.printHelp("kafka-src", options);
      System.exit(1);
      return;
    }

    final String kafkaBrokerAddress = cmd.getOptionValue("b");
    final int tupleNum = Integer.valueOf(cmd.getOptionValue("n"));
    final int numKeys = Integer.valueOf(cmd.getOptionValue("k"));
    final double skewness = Double.valueOf(cmd.getOptionValue("s"));

    // Instantiate kafka producer
    final Properties props = new Properties();
    props.put("bootstrap.servers", kafkaBrokerAddress);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    final long startTime = System.currentTimeMillis();
    final Producer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
    final ZipfWordGenerator wordGenerator = new ZipfWordGenerator(numKeys, skewness);
    for (int i = 0; i < tupleNum; i++) {
      kafkaProducer.send(new ProducerRecord<>("word", wordGenerator.getNextWord()));
    }
    System.out.println(String.format("%d events are sent in %f seconds...", tupleNum,
        (System.currentTimeMillis() - startTime) / 1000.));
    System.exit(0);
  }
}