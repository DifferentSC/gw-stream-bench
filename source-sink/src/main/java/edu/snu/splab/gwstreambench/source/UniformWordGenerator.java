package edu.snu.splab.gwstreambench.source;

import java.util.Random;

/**
 * Created by Gyewon on 03/12/2018.
 */
public class UniformWordGenerator implements WordGenerator {

  private final int numKeys;

  private Random random;

  public UniformWordGenerator(final int numKeys) {
    this.numKeys = numKeys;
    this.random = new Random();
  }

  @Override
  public String getNextWord() {
    return String.valueOf(random.nextInt(numKeys));
  }
}
