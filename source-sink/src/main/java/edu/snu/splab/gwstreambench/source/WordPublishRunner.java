package edu.snu.splab.gwstreambench.source;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Random;
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

  private final int eventsEmitsPerBatch;

  private final WordGenerator wordGenerator;

  private final List<String> marginList;

  private final Random random;

  private Long timeDiff;

  public WordPublishRunner(
    final Producer<String, String> kafkaProducer,
    final WordGenerator wordGenerator,
    final String topicName,
    final int eventsEmitsPerBatch,
    final List<String> marginList,
    final long timeDiff
  ) {
    this.kafkaProducer = kafkaProducer;
    this.wordGenerator = wordGenerator;
    this.topicName = topicName;
    this.eventsEmitsPerBatch = eventsEmitsPerBatch;
    this.marginList = marginList;
    this.random = new Random();
    this.timeDiff = timeDiff;
  }

  @Override
  public void run() {
    for (int i = 0; i < eventsEmitsPerBatch; i++) {
      final String word = wordGenerator.getNextWord();
      final String margin = marginList.get(random.nextInt(marginList.size()));
      final Long timestamp = System.currentTimeMillis() - this.timeDiff;
      final String event = String.format("%s %s %d", word, margin, timestamp);
      kafkaProducer.send(new ProducerRecord<>("word", event));
    }
  }
}
