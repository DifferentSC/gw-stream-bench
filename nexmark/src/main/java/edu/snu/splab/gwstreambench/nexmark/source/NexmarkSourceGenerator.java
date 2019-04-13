package edu.snu.splab.gwstreambench.nexmark.source;

import edu.snu.splab.gwstreambench.nexmark.model.Event;

import java.util.Iterator;

public final class NexmarkSourceGenerator implements Iterator<Event> {
    private static final int NUM_GENERATORS = 1;
    private static final int PERSON_PROPORTION = 1;
    private static final int AUCTION_PROPORTION = 3;
    private static final int BID_PROPORTION = 46;
    private static final int PROPORTION_DENOMINATOR =
            PERSON_PROPORTION + AUCTION_PROPORTION + BID_PROPORTION;
    private static final int EVENTS_PER_SECOND = 10000;
    private static final long INTER_EVENT_DELAY_US
            = ((1_000_000L + EVENTS_PER_SECOND / 2) / EVENTS_PER_SECOND) * NUM_GENERATORS;

    private long numGeneratedEvents = 0;

    @Override
    public boolean hasNext() {
        // Always has next (streaming!)
        return true;
    }

    @Override
    public Event next() {
        final long newEventId = numGeneratedEvents * 954;
        numGeneratedEvents++;
        return null;
    }
}
