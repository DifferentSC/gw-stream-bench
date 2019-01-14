package edu.snu.splab.gwstreambench.source;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The session word generator with uniform distribution.
 */
public class UniformSessionWordGenerator implements WordGenerator {

  private final int numKeys;

  private final int averageSessionTerm;

  private final int sessionGap;

  private final Map<Integer, AtomicBoolean> activeKeyMap;

  private final ScheduledExecutorService scheduledExecutorService;

  private Random random;

  UniformSessionWordGenerator(
      final int numKeys,
      final int averageSessionTerm,
      final int sessionGap,
      final int timerThreadNum
  ) {
    this.numKeys = numKeys;
    this.averageSessionTerm = averageSessionTerm;
    this.sessionGap = sessionGap;
    this.scheduledExecutorService = Executors.newScheduledThreadPool(timerThreadNum);
    this.activeKeyMap = new HashMap<>();
    this.random = new Random();
    for (int i = 0; i < numKeys; i++) {
      activeKeyMap.put(i, new AtomicBoolean(true));
      final int sessionTerm = random.nextInt(averageSessionTerm * 2);
      scheduledExecutorService.schedule(new SessionInactiveRunner(i), sessionTerm, TimeUnit.SECONDS);
    }
  }

  @Override
  public String getNextWord() {
    int selectedKey = random.nextInt(numKeys);
    // Repeat until getting the active keys.
    while (!activeKeyMap.get(selectedKey).get()) {
      selectedKey = random.nextInt(numKeys);
    }
    return String.valueOf(selectedKey);
  }

  private class SessionActiveRunner extends TimerTask {

    private final int word;

    SessionActiveRunner(final int word) {
      this.word = word;
    }

    @Override
    public void run() {
      activeKeyMap.get(word).compareAndSet(false, true);
      final int sessionTerm = random.nextInt(averageSessionTerm) * 2;
      scheduledExecutorService.schedule(new SessionInactiveRunner(word), sessionTerm, TimeUnit.SECONDS);
    }
  }

  private class SessionInactiveRunner extends TimerTask {

    private int word;

    SessionInactiveRunner(final int word) {
      this.word = word;
    }

    @Override
    public void run() {
      activeKeyMap.get(word).compareAndSet(true, false);
      scheduledExecutorService.schedule(new SessionActiveRunner(word), sessionGap, TimeUnit.SECONDS);
    }
  }
}
