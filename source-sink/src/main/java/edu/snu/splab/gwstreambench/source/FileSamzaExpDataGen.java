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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The file generator for samza experiment.
 */
public class FileSamzaExpDataGen {

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

    final Option marginOption = new Option("m", true, "The size of string margin.");
    marginOption.setRequired(true);
    options.addOption(marginOption);

    final CommandLineParser parser = new DefaultParser();
    final HelpFormatter formatter = new HelpFormatter();
    final CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (final ParseException e) {
      System.err.println(e);
      formatter.printHelp("file-exp-data-generator", options);
      System.exit(1);
      return;
    }

    final String filePathString = cmd.getOptionValue("f");
    final long tupleNum = Long.valueOf(cmd.getOptionValue("n"));
    final long numKeys = Long.valueOf(cmd.getOptionValue("k"));
    final double skewness = Double.valueOf(cmd.getOptionValue("s"));
    final int margin = Integer.valueOf(cmd.getOptionValue("m"));

    final Random random = new Random();
    final Path filePath = Paths.get(filePathString);
    //final ZipfWordGenerator wordGenerator = new ZipfWordGenerator(numKeys, skewness);
    final BufferedWriter bufferedWriter = Files.newBufferedWriter(filePath, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

    final List<String> marginList = new ArrayList<>();
    for (int i = 0; i < 1000; i ++) {
      final byte[] marginBytes = new byte[margin];
      for (int j = 0; j < margin; j++) {
        marginBytes[j] = (byte) (random.nextInt(26) + 'a');
      }
      marginList.add(new String(marginBytes));
    }
    System.out.println("Finished generating random margin bytes...");

    for (int i = 0; i < tupleNum; i++) {
      final String marginString = marginList.get(random.nextInt(1000));
      long key = ThreadLocalRandom.current().nextLong(numKeys);
      bufferedWriter.write(key + " " + marginString + "\n");
    }
    bufferedWriter.flush();
    bufferedWriter.close();
  }
}
