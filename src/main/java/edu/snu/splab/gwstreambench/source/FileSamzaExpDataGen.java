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
    final int tupleNum = Integer.valueOf(cmd.getOptionValue("n"));
    final int numKeys = Integer.valueOf(cmd.getOptionValue("k"));
    final double skewness = Double.valueOf(cmd.getOptionValue("s"));
    final int margin = Integer.valueOf(cmd.getOptionValue("m"));

    final Random random = new Random();
    final Path filePath = Paths.get(filePathString);
    final ZipfWordGenerator wordGenerator = new ZipfWordGenerator(numKeys, skewness);
    final BufferedWriter bufferedWriter = Files.newBufferedWriter(filePath, StandardOpenOption.CREATE);

    for (int i = 0; i < tupleNum; i++) {
      final byte[] byteArray = new byte[margin + 1];
      for (int j = 0; j < margin; j++) {
        byteArray[j] = (byte) (random.nextInt(26) + 'a');
      }
      byteArray[margin] = '\0';
      bufferedWriter.write(wordGenerator.getNextWord() + " " + new String(byteArray));
    }
    bufferedWriter.flush();
    bufferedWriter.close();
  }
}
