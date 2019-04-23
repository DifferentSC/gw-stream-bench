package edu.snu.splab.gwstreambench.nexmark.model;

public class Event {
    public enum EventType {
        PERSON,
        AUCTION,
        BID
    }

    public EventType eventType;
    public Person person;
    public Auction auction;
    public Bid bid;
    // event timestamp (specified as milliseconds since the Java epoch of 1970-01-01T00:00:00Z)
    public long timestamp;

    public Event() {
    }
}
