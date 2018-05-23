package edu.snu.splab.gwstreambench.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Created by Gyewon on 2018. 4. 9..
 */
public class ZipfWordGenerator {

  private final List<String> wordList;

  private final List<Double> zipfProbList;

  private Random random;

  public ZipfWordGenerator(int numKeys, double alpha) {
    this.wordList = new ArrayList<String>();
    this.zipfProbList = new ArrayList<Double>();
    double sumProb = 0.;
    for (int i = 1; i <= numKeys; i++) {
      sumProb += Math.pow(1./(double)i, alpha);
    }
    final double mostPopularProb = 1. / sumProb;

    for (int i = 1; i <= numKeys; i++) {
      wordList.add(String.valueOf(i));
      // Add the normalized probability.
      zipfProbList.add(mostPopularProb * Math.pow(1./(double)i, alpha));
    }
    // shuffle the word list
    Collections.shuffle(wordList);
    this.random = new Random();
  }

  public String getNextWord() {
    double randomDouble = random.nextDouble();
    double cumulativeProb = 0.0;
    int selectedIndex = -1;
    while (cumulativeProb < randomDouble) {
      selectedIndex++;
      cumulativeProb += zipfProbList.get(selectedIndex);
    }
    return wordList.get(selectedIndex);
  }
}
