package edu.snu.splab.gwstreambench.simul;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
//import org.apache.flink.contrib.streaming.state.io.InputStreamDataInputView;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;

public class LogFileStore<K> {
  static Path savedMaxTimeStampFilePath;
  static Path metadataLogFilePath;
  static Path logFilePath;

  static Map<Integer, List<byte[]>> writeBuffer = new ConcurrentHashMap<>();
  static int pendingWrites = 0;

  //Constructor
  public LogFileStore(
    Path savedMaxTimeStampFilePath,
    Path metadataLogFilePath,
    Path logFilePath
  ) {
    this.savedMaxTimeStampFilePath = savedMaxTimeStampFilePath;
    this.metadataLogFilePath = metadataLogFilePath;
    this.logFilePath = logFilePath;
  }

  static void write(Integer currentKey, final byte[] currentElement) {
      writeBuffer.computeIfAbsent(currentKey, (k) -> new ArrayList<byte[]>());
      List<byte[]> wbForKey = writeBuffer.get(currentKey);
      if (wbForKey == null)
        wbForKey = new ArrayList<>();

      if (currentElement == null) {
        wbForKey.clear();
        synchronized (writeBuffer) {
          wbForKey.add(null);
        }
      } else {
        synchronized (writeBuffer) {
          wbForKey.add(currentElement);
          System.out.println("wbforkey: " + wbForKey + "writeBuffer: " + writeBuffer.get(currentKey));
        }
        pendingWrites += 1;
        if (pendingWrites > 10000) {
          clearWriteBuffer();
        }
      }
  }

  static void clearWriteBuffer() {
    try (final DataOutputStream metadataFileOut = new DataOutputStream(new BufferedOutputStream(
      new FileOutputStream(metadataLogFilePath.toFile(), true)));
         final BufferedOutputStream groupFileOut = new BufferedOutputStream(
           new FileOutputStream(logFilePath.toFile(), true))
    ) {
      for (final Map.Entry<Integer, List<byte[]>> entry : writeBuffer.entrySet()) {
        final int key = entry.getKey();

        final byte[] serializedKey = LargeScaleWindowSimul.serializedKeys.get(key);

        int size = 0;
        long currentPos = Files.size(logFilePath);

        for (final byte[] serializedData : entry.getValue()) {

          if (serializedData == null) {
            // Write triggers
            metadataFileOut.write(serializedKey);
            metadataFileOut.writeLong(-1L);
            metadataFileOut.writeInt(-1);
          } else {
            groupFileOut.write(serializedData.length / 256);
            groupFileOut.write(serializedData.length % 256);
            // Write to value log file.
            groupFileOut.write(serializedData);
            size += serializedData.length + 2;
          }
        }
        if (size != 0) {
          // Write to metadata log file.
          metadataFileOut.write(serializedKey);
          metadataFileOut.writeLong(currentPos);
          metadataFileOut.writeInt(size);
          currentPos += size;
        }
      }
    } catch (final IOException e) {
      final StringBuilder builder = new StringBuilder();
      for (final StackTraceElement element : e.getStackTrace()) {
        builder.append(element);
        builder.append('\n');
      }
      throw new FlinkRuntimeException("Exception occurred while writing log files! " + builder.toString());
    } finally {
      writeBuffer.clear();
      pendingWrites = 0;
    }
  }

  static void writeTimestampToFile(ArrayList<Integer> keysofThisSubtask, int groupNum, HashMap<Integer, Long> keyToMaxTimestamp) {
    //write max timestamp to file
    try (final DataOutputStream timestampFileOut = new DataOutputStream(new BufferedOutputStream(
      new FileOutputStream(savedMaxTimeStampFilePath.toFile(), true)));
    ) {
      for (Integer i = 0; i < keysofThisSubtask.size(); i++) {
        final Integer key = keysofThisSubtask.get(i);
        if (key % LargeScaleWindowSimul.groupNum == groupNum) //if the key inside this subtask, belongs to THIS groupNum => write to this file
        {
          final Long maxTimestamp = keyToMaxTimestamp.get(key);

          timestampFileOut.write(key);
          //LargeScaleWindowSimul.timestampSerializer.serialize((long) maxTimestamp, LargeScaleWindowSimul.timestampSerializationDataOutputView);
          //final byte[] serializedMaxTimestamp = LargeScaleWindowSimul.timestampSerializationStream.toByteArray();
          timestampFileOut.write(LargeScaleWindowSimul.serializedTimestamps.get((int) (long) maxTimestamp));
        }
      }
    } catch (final IOException e) {
      final StringBuilder builder = new StringBuilder();
      for (final StackTraceElement element : e.getStackTrace()) {
        builder.append(element);
        builder.append('\n');
      }
      throw new FlinkRuntimeException("Exception occurred while writing log files! " + builder.toString());
    }
  }

}
