package edu.snu.splab.gwstreambench.simul;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.ArrayUtils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.FlinkRuntimeException;

import org.checkerframework.checker.units.qual.K;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

import akka.japi.Pair;

import org.apache.commons.io.FileUtils;

public class LargeScaleWindowSimul {

  static ArrayList<byte[]> serializedKeys = new ArrayList<>();

  //params
  static int windowSize;
  static int numKeys;
  static int marginSize;
  static int groupNum;
  static int numThreads;
  static int dataRate;
  static int averageSessionTerm;
  static int sessionGap;
  static int inactiveTime;
  static String stateStorePath;

  //serialized related variables
  static TypeSerializer<Integer> keySerializer;
  static ByteArrayOutputStreamWithPos keySerializationStream;
  static DataOutputView keySerializationDataOutputView;

  static TypeSerializer<String> marginSerializer;
  static ByteArrayOutputStreamWithPos marginSerializationStream;
  static DataOutputView marginSerializationDataOutputView;
  static ArrayList<byte[]> serializedMargins;

  static TypeSerializer<Long> timestampSerializer;
  static ByteArrayOutputStreamWithPos timestampSerializationStream;
  static DataOutputView timestampSerializationDataOutputView;
  static ArrayList<byte[]> serializedTimestamps;


  public static final void main(final String[] args) throws Exception {

    System.out.println("Getting options...");

    final Options options = new Options();

    final Option windowSizeOpt = new Option("w", true, "window_size");
    windowSizeOpt.setRequired(true);
    options.addOption(windowSizeOpt);

    final Option numKeysOption = new Option("k", true, "num_keys");
    numKeysOption.setRequired(true);
    options.addOption(numKeysOption);

    final Option marginSizeOption = new Option("m", true, "margin_size");
    marginSizeOption.setRequired(true);
    options.addOption(marginSizeOption);

    final Option groupNumOption = new Option("g", true, "group_num");
    groupNumOption.setRequired(true);
    options.addOption(groupNumOption);

    final Option numThreadsOption = new Option("t", true, "num_threads");
    numThreadsOption.setRequired(true);
    options.addOption(numThreadsOption);

    final Option dataRateOption = new Option("d", true, "data_rate");
    dataRateOption.setRequired(false);
    options.addOption(dataRateOption);

    final Option avgSessionTermOption = new Option("ast", true, "average_session_term");
    avgSessionTermOption.setRequired(true);
    options.addOption(avgSessionTermOption);

    final Option sessionGapOption = new Option("sg", true, "session_gap");
    sessionGapOption.setRequired(false);
    options.addOption(sessionGapOption);

    final Option inactiveTimeOption = new Option("i", true, "inactive_time");
    inactiveTimeOption.setRequired(false);
    options.addOption(inactiveTimeOption);

    final Option stateStorePathOption = new Option("sst", true, "state_store_path");
    stateStorePathOption.setRequired(false);
    options.addOption(stateStorePathOption);

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

    windowSize = Integer.valueOf(cmd.getOptionValue("w"));
    numKeys = Integer.valueOf(cmd.getOptionValue("k"));
    marginSize = Integer.valueOf(cmd.getOptionValue("m"));
    groupNum = Integer.valueOf(cmd.getOptionValue("g"));
    numThreads = Integer.valueOf(cmd.getOptionValue("t"));
    dataRate = Integer.valueOf(cmd.getOptionValue("d"));
    averageSessionTerm = Integer.valueOf(cmd.getOptionValue("ast"));
    sessionGap = Integer.valueOf(cmd.getOptionValue("sg"));
    inactiveTime = Integer.valueOf(cmd.getOptionValue("i"));
    stateStorePath = cmd.getOptionValue("sst");


    keySerializer = new IntSerializer();
    keySerializationStream = new ByteArrayOutputStreamWithPos(128);
    keySerializationDataOutputView = new DataOutputViewStreamWrapper(keySerializationStream);

    marginSerializer = new StringSerializer();
    marginSerializationStream = new ByteArrayOutputStreamWithPos(128);
    marginSerializationDataOutputView = new DataOutputViewStreamWrapper(marginSerializationStream);
    serializedMargins = new ArrayList<>();

    timestampSerializer = new LongSerializer();
    timestampSerializationStream = new ByteArrayOutputStreamWithPos(128);
    timestampSerializationDataOutputView = new DataOutputViewStreamWrapper(timestampSerializationStream);
    serializedTimestamps = new ArrayList<>();

    //generate margins
    final Random random = new Random();
    final List<String> marginList = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      final byte[] marginBytes = new byte[marginSize];
      for (int j = 0; j < marginSize; j++) {
        marginBytes[j] = (byte) (random.nextInt(26) + 'a');
      }
      marginList.add(new String(marginBytes));
    }

    for (int i = 0; i < numKeys; i++) {
      //serialize keys
      try {
        keySerializer.serialize(i, keySerializationDataOutputView);
      } catch (IOException e) {
        e.printStackTrace();
      }
      keySerializationStream.reset();
      final byte[] serializedKey = keySerializationStream.toByteArray();
      //System.out.println("serializedKey: "+serializedKey+"(length:"+serializedKey.length+")");
      serializedKeys.add(serializedKey);

      //serialize margins
      try {
        marginSerializationStream.reset();
        marginSerializer.serialize(marginList.get(random.nextInt(marginList.size())), marginSerializationDataOutputView);
      } catch (IOException e) {
        e.printStackTrace();
      }
      final byte[] serializedMargin = marginSerializationStream.toByteArray();
      //System.out.println("serializedMargin: "+serializedMargin+"(length:"+serializedMargin.length+")");
      serializedMargins.add(serializedMargin);
    }

    System.out.println("generate timestamps");
    for (int i = 0; i < windowSize * 1000; i++) {

      //serialize timestamps
      try {
        timestampSerializationStream.reset();
        timestampSerializer.serialize((long) i, timestampSerializationDataOutputView);
      } catch (IOException e) {
        e.printStackTrace();
      }
      final byte[] serializedTimestamp = timestampSerializationStream.toByteArray();
      //System.out.println("serializedTimestamp: "+serializedTimestamp+"(length:"+serializedTimestamp.length+")");
      serializedTimestamps.add(serializedTimestamp);
    }

    //create log, metadata, timestamp files
    //subtask index:0~7, groupNum:0~3(%4)
    final String META_DATA_LOG_FILE_NAME_FORMAT = "%d.meta.log";
    final String LOG_FILE_NAME_FORMAT = "%d.data.log";
    final String SAVED_MAX_TIMESTAMP_FILE_NAME_FORMAT = "%d.maxTimestamp.log";

    System.out.println("create files..");
    for (int i = 0; i < numThreads; i++)//subtask index
    {
      Path logFileDirectoryPath = Paths.get("/nvme", String.valueOf(i), "window-contents-separate-triggers");

      //delete all files in directory
      System.out.println("clean directory..");
      if(Files.exists(logFileDirectoryPath))
      {
        try {
            Files.walkFileTree(logFileDirectoryPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
              System.out.println("delete file: " + file.toString());
              Files.delete(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
              Files.delete(dir);
              System.out.println("delete dir: " + dir.toString());
              return FileVisitResult.CONTINUE;
            }
          });
        } catch(IOException e){
          e.printStackTrace();
        }
      }


      Files.createDirectories(logFileDirectoryPath);
      //System.out.println("clean directory..");
      //FileUtils.cleanDirectory(logFileDirectoryPath.getFileName());

      for (int j = 0; j < groupNum; j++)//group number
      {
        String groupFileName = String.format(LOG_FILE_NAME_FORMAT, j);
        String metadataFileName = String.format(META_DATA_LOG_FILE_NAME_FORMAT, j);
        String maxTimeStampFileName = String.format(SAVED_MAX_TIMESTAMP_FILE_NAME_FORMAT, j);

        Path logFilePath = Paths.get(logFileDirectoryPath.toString(), groupFileName);
        Path metadataLogFilePath = Paths.get(logFileDirectoryPath.toString(), metadataFileName);
        Path savedMaxTimeStampFilePath = Paths.get(logFileDirectoryPath.toString(), maxTimeStampFileName);

        try {
          if (!Files.exists(logFilePath)) {
            Files.createFile(logFilePath);
          }

          if (!Files.exists(metadataLogFilePath)) {
            Files.createFile(metadataLogFilePath);
          }

          if (!Files.exists(savedMaxTimeStampFilePath)) {
            Files.createFile(savedMaxTimeStampFilePath);
          }

        } catch (final IOException e) {
          throw new FlinkRuntimeException("Failed to create file", e);
        }
      }
    }

    System.out.println("\nRead newpath txt...");
    //per subtask, array of keys belonging to it
    Map<Integer, ArrayList<Integer>> subtaskKeys = new HashMap<>();
    try {
      File file = new File("newpath.txt");
      FileReader filereader = new FileReader(file);
      BufferedReader bufReader = new BufferedReader(filereader);
      String line = "";

      while ((line = bufReader.readLine()) != null) {
        String[] words = line.split(" ");//words[0]:key, words[1]:subtask number

        int subtask, key;
        key = Integer.parseInt(words[0]);
        subtask = Integer.parseInt(words[1]);

        //add keys to map
        if (subtaskKeys.get(subtask) == null) {
          ArrayList<Integer> tmpArr = new ArrayList<>();
          tmpArr.add(key);
          subtaskKeys.put(subtask, tmpArr);
        } else {
          if (!subtaskKeys.get(subtask).contains(key))//append
            subtaskKeys.get(subtask).add(key);
        }
      }
      bufReader.close();

    } catch (FileNotFoundException e) {
      throw new FlinkRuntimeException("Cannot find file", e);
      //System.out.println(e);
    }

    System.out.println("\nCreate threads");
    Thread[] threads = new Thread[numThreads];//number of subtasks
    //create threads
    for (int i = 0; i < numThreads; i++) {
      //per thread, 1 subtask
      Runnable r = new Worker(i, subtaskKeys.get(i));//subtask num & keys belonging to it
      threads[i] = new Thread(r);
      threads[i].start();
    }

    //join threads
    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }
  }


}
