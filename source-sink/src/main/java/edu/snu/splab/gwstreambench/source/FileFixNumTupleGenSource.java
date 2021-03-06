package edu.snu.splab.gwstreambench.source;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

/**
 * Write fixed number of tuples to a file.
 */
public class FileFixNumTupleGenSource {

  public static void main(final String[] args) throws Exception {
    final Options options = new Options();

    final Option kafkaBrokerAddressOpt = new Option("f", true,"The file path to be written.");
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

    final String filePathString = cmd.getOptionValue("f");
    final int tupleNum = Integer.valueOf(cmd.getOptionValue("n"));
    final int numKeys = Integer.valueOf(cmd.getOptionValue("k"));
    final double skewness = Double.valueOf(cmd.getOptionValue("s"));
    final Random random = new Random();

    final Path filePath = Paths.get(filePathString);
    final ZipfWordGenerator wordGenerator = new ZipfWordGenerator(numKeys, skewness);
    final BufferedWriter bufferedWriter = Files.newBufferedWriter(filePath, StandardOpenOption.CREATE);
    for (int i = 0; i < tupleNum; i++) {
      bufferedWriter.write(wordGenerator.getNextWord() + "\n");
    }
    bufferedWriter.flush();
    bufferedWriter.close();
  }
}
