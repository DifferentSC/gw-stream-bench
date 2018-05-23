package edu.snu.splab.gwstreambench.source;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.TimerTask;

/**
 * Created by Gyewon on 2018. 4. 9..
 */
public class WordPublishRunner extends TimerTask {

  /**
   * The Kafka producer.
   */
  private final Producer<String, String> kafkaProducer;

  /**
   * The publish kafka topic name.
   */
  private final String topicName;

  private int eventsEmitsPerBatch;

  private ZipfWordGenerator wordGenerator;

  public WordPublishRunner(
      final Producer<String, String> kafkaProducer,
      final ZipfWordGenerator wordGenerator,
      final String topicName,
      final int eventsEmitsPerBatch
      ) {
    this.kafkaProducer = kafkaProducer;
    this.wordGenerator = wordGenerator;
    this.topicName = topicName;
    this.eventsEmitsPerBatch = eventsEmitsPerBatch;
  }

  @Override
  public void run() {
    for (int i = 0; i < eventsEmitsPerBatch; i++) {
      final String word = wordGenerator.getNextWord();
      kafkaProducer.send(new ProducerRecord<String, String>("word", word));
    }
  }
}
