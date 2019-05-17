package edu.snu.splab.gwstreambench.simul;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import akka.japi.Pair;

public class LargeScaleWindowSimul {

    static ArrayList<byte[]> serializedKeys =new ArrayList<>();

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
    //static ArrayList<byte[]> serializedTimestamps;


    public static final void main(final String[] args) throws Exception {
	System.out.println("in main");

        try{
            final ParameterTool params = ParameterTool.fromArgs(args);
            windowSize = params.getInt("window_size");
            numKeys = params.getInt("num_keys");
            marginSize = params.getInt("margin_size");
            groupNum = params.getInt("group_num");
            numThreads = params.getInt("num_threads");
            dataRate = params.getInt("data_rate");
            averageSessionTerm = params.getInt("average_session_term");
            sessionGap = params.getInt("session_gap");
            inactiveTime = params.getInt("inactive_time");
            stateStorePath = params.get("state_store_path");

        }catch(final Exception e){
            System.err.println("Missing configuration!" + e.toString());
            return;
        }
	
	System.out.println("after paramers");
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
        //serializedTimestamps =new ArrayList<>();

        //generate margins
        final Random random = new Random();
        final List<String> marginList = new ArrayList<>();
        for (int i = 0; i < 10000; i ++) {
            final byte[] marginBytes = new byte[marginSize];
            for (int j = 0; j < marginSize; j++) {
                marginBytes[j] = (byte) (random.nextInt(26) + 'a');
            }
            marginList.add(new String(marginBytes));
        }

        for(int i = 0; i < numKeys; i++) {
            //serialize keys
            try{
                keySerializer.serialize(i, keySerializationDataOutputView);
            } catch(IOException e){
                e.printStackTrace();
            }
            final byte[] serializedKey = keySerializationStream.toByteArray();
            serializedKeys.add(serializedKey);

            //serialize margins
            try{
                marginSerializer.serialize(marginList.get(random.nextInt(marginList.size())), marginSerializationDataOutputView);
            } catch(IOException e){
                e.printStackTrace();
            }
            final byte[] serializedMargin = marginSerializationStream.toByteArray();
            serializedMargins.add(serializedMargin);
        }

	System.out.println("generate timestamps");
	/*
	for(int i = 0; i < windowSize * 1000; i++) {

            //serialize timestamps
            try {
                timestampSerializer.serialize((long) i, timestampSerializationDataOutputView);
            } catch(IOException e){
                e.printStackTrace();
            }
            final byte[] serializedTimestamp = timestampSerializationStream.toByteArray();
            serializedTimestamps.add(serializedTimestamp);
        }*/
	

        //create log, metadata, timestamp files
        //subtask index:0~7, groupNum:0~3(%4)
        final String META_DATA_LOG_FILE_NAME_FORMAT = ".meta.log";
        final String LOG_FILE_NAME_FORMAT = ".data.log";
        final String SAVED_MAX_TIMESTAMP_FILE_NAME_FORMAT = ".maxTimestamp.log";

	System.out.println("create files..");
        for(int i=0; i < numThreads ; i++)//subtask index
        {
            for(int j=0; j < groupNum ; j++)//group number
            {
                String groupFileName = String.format(LOG_FILE_NAME_FORMAT, String.valueOf(j));
                String metadataFileName = String.format(META_DATA_LOG_FILE_NAME_FORMAT, String.valueOf(j));
                String maxTimeStampFileName = String.format(SAVED_MAX_TIMESTAMP_FILE_NAME_FORMAT, String.valueOf(j));

                Path logFileDirectoryPath = Paths.get("/nvme",String.valueOf(i), "window-contents-separate-triggers");
                Path logFilePath = Paths.get(logFileDirectoryPath.toString(), groupFileName);
                Path metadataLogFilePath = Paths.get(logFileDirectoryPath.toString(), metadataFileName);
                Path savedMaxTimeStampFilePath = Paths.get(logFileDirectoryPath.toString(), maxTimeStampFileName);

                Files.createDirectories(logFilePath.getParent());
                Files.createFile(metadataLogFilePath);
                Files.createFile(logFilePath);
                Files.createFile(savedMaxTimeStampFilePath );
            }
        }

	System.out.println("read newpath txt");
        //per subtask, array of keys belonging to it
        Map<Integer, ArrayList<Integer>> subtaskKeys = new HashMap<>();
        try{
	    System.out.println("opening newpath file....");
            File file = new File("/newpath.txt");
            FileReader filereader = new FileReader(file);
            BufferedReader bufReader = new BufferedReader(filereader);
            String line = "";

            while((line = bufReader.readLine()) != null){
                String[] words = line.split(" ");//words[0]:key, words[1]:subtask number

                int subtask, key;
                key=Integer.parseInt(words[0]);
                subtask=Integer.parseInt(words[1]);

                //add keys to map
                if(subtaskKeys.get(subtask) == null)
                {
                    ArrayList<Integer> tmpArr = new ArrayList<>();
                    tmpArr.add(key);
                    subtaskKeys.put(subtask, tmpArr);
                }
                else
                {
                    if(!subtaskKeys.get(subtask).contains(key))//append
                        subtaskKeys.get(subtask).add(key);
                }
            }
            bufReader.close();

        }catch (FileNotFoundException e) {
            throw new FlinkRuntimeException("Cannot find file", e);
            //System.out.println(e);
        }

	System.out.println("create threads");
        Thread[] threads = new Thread[numThreads];//number of subtasks
        //create threads
        for(int i=0;i < numThreads; i++)
        {
            //per thread, 1 subtask
            Runnable r = new Worker(i, subtaskKeys.get(i));//subtask num & keys belonging to it
            threads[i] = new Thread(r);
            threads[i].start();
        }

        //join threads
        for(int i=0;i < numThreads; i++) {
            threads[i].join();
        }
    }


}
