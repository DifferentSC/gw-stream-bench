package edu.snu.splab.gwstreambench.simul;

import akka.japi.Pair;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Random;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Worker implements Runnable {
  final String META_DATA_LOG_FILE_NAME_FORMAT = "%d.meta.log";
  final String LOG_FILE_NAME_FORMAT = "%d.data.log";
  final String SAVED_MAX_TIMESTAMP_FILE_NAME_FORMAT = "%d.maxTimestamp.log";

  int subtaskNum;
  ArrayList<Integer> keysofThisSubtask;
  int dataRate;

  public Worker(int subtaskNum, ArrayList<Integer> keysofThisSubtask) {
    this.subtaskNum = subtaskNum;
    this.keysofThisSubtask = keysofThisSubtask;
    if (subtaskNum == (LargeScaleWindowSimul.numThreads - 1))//if last one, take care of remainders of total dataRate
      this.dataRate = LargeScaleWindowSimul.dataRate - (LargeScaleWindowSimul.numThreads - 1) * (LargeScaleWindowSimul.dataRate / LargeScaleWindowSimul.numThreads);
    else
      this.dataRate = LargeScaleWindowSimul.dataRate / LargeScaleWindowSimul.numThreads;
  }

  @Override
  public void run() {
    System.out.println("worker run");
    //create LogFileStore Instances for this subtask
    ArrayList<LogFileStore> logFiles = new ArrayList<>();
    for (int j = 0; j < LargeScaleWindowSimul.groupNum; j++)//group number
    {
      String groupFileName = String.format(LOG_FILE_NAME_FORMAT, j);
      String metadataFileName = String.format(META_DATA_LOG_FILE_NAME_FORMAT, j);
      String maxTimeStampFileName = String.format(SAVED_MAX_TIMESTAMP_FILE_NAME_FORMAT, j);

      Path logFileDirectoryPath = Paths.get("/nvme", String.valueOf(this.subtaskNum), "window-contents-separate-triggers");
      Path logFilePath = Paths.get(logFileDirectoryPath.toString(), groupFileName);
      Path metadataLogFilePath = Paths.get(logFileDirectoryPath.toString(), metadataFileName);
      Path savedMaxTimeStampFilePath = Paths.get(logFileDirectoryPath.toString(), maxTimeStampFileName);

      logFiles.add(new LogFileStore(savedMaxTimeStampFilePath, metadataLogFilePath, logFilePath));
    }

    //Data Generation
    final Random random = new Random();
    HashMap<Integer, Pair<Long, Long>> activeTimeMap = new HashMap<>();//<key, <active start time, active end time>>
    HashMap<Integer, Long> keyToMaxTimestamp = new HashMap<>();

    //initially all keys are active
    //& max timestamp of all keys are -1
    for (int i = 0; i < LargeScaleWindowSimul.numKeys; i++) {

      Pair<Long, Long> pair = new Pair<>(new Long(0), new Long(random.nextInt(LargeScaleWindowSimul.averageSessionTerm * 2)));
      activeTimeMap.put(i, pair);
      keyToMaxTimestamp.put(i, (long) -1);
    }

    for (long timestamp = 0; timestamp < LargeScaleWindowSimul.windowSize * 1000; ) {
      for (int j = 0; j < this.dataRate; j++) {
        int selectedKey;
        while (true) {
          selectedKey = keysofThisSubtask.get(random.nextInt(keysofThisSubtask.size()));

          //if selected key has expired active period
          if (activeTimeMap.get(selectedKey).second() <= timestamp) {
            //if selected key has been triggered(session gap passed), write read marker
            if (timestamp - activeTimeMap.get(selectedKey).second() >= LargeScaleWindowSimul.sessionGap) {
              logFiles.get(selectedKey % LargeScaleWindowSimul.groupNum).write(selectedKey, null);
            }

            //defer selected key's active time
            Long newFirst = activeTimeMap.get(selectedKey).second() + LargeScaleWindowSimul.inactiveTime;
            Pair<Long, Long> pair = new Pair<>(newFirst, newFirst + random.nextInt(LargeScaleWindowSimul.averageSessionTerm * 2));
            activeTimeMap.put(selectedKey, pair);
            continue;
          }

          //found a key that is in active period
          if (activeTimeMap.get(selectedKey).first() <= timestamp && activeTimeMap.get(selectedKey).second() >= timestamp) {
            break;
          }
        }
        //this key is active

        byte[] a = LargeScaleWindowSimul.serializedKeys.get(selectedKey);
        byte[] b = LargeScaleWindowSimul.serializedMargins.get(random.nextInt(LargeScaleWindowSimul.numKeys));
        byte[] c = LargeScaleWindowSimul.serializedTimestamps.get((int) (long) timestamp);
        byte[] serializedElement = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0 , serializedElement, 0, a.length);
        System.arraycopy(b, 0 , serializedElement, a.length, b.length);
        System.arraycopy(c, 0 , serializedElement, a.length+b.length, c.length);

        logFiles.get(selectedKey % LargeScaleWindowSimul.groupNum).write(selectedKey, serializedElement);

        //if it is max timestamp of the key, save it to keyToMaxTimestamp
        if (keyToMaxTimestamp.get(selectedKey) < timestamp) {
          keyToMaxTimestamp.put(selectedKey, timestamp);
        }

        if (j == this.dataRate - 1)//if made all data, move to next second
          timestamp = ((timestamp / 1000) + 1) * 1000;
        else
          timestamp += 1000 / this.dataRate;
      }
    }

    System.out.println("\nflush~");
    //Finally, handle unwritten requests
    for (int j = 0; j < LargeScaleWindowSimul.groupNum; j++)//group number
    {
      //flush unwritten pending writes in this LogFileStore
      logFiles.get(j).clearWriteBuffer();

      //write max timestamp to file
      logFiles.get(j).writeTimestampToFile(keysofThisSubtask, j, keyToMaxTimestamp);
    }

  }

}
