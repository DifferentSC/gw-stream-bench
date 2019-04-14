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
    public long watermark;

    public Event() {
    }

    public Event(final EventType eventType, final Person person, final Auction auction, final Bid bid,
                 final long timestamp, final long watermark) {
        this.eventType = eventType;
        this.person = person;
        this.auction = auction;
        this.bid = bid;
        this.timestamp = timestamp;
        this.watermark = watermark;
    }

    public Event(final Person person, final long timestamp, final long watermark) {
        this(EventType.PERSON, person, null, null, timestamp, watermark);
    }

    public Event(final Auction auction, final long timestamp, final long watermark) {
        this(EventType.AUCTION, null, auction, null, timestamp, watermark);
    }

    public Event(final Bid bid, final long timestamp, final long watermark) {
        this(EventType.BID, null, null, bid, timestamp, watermark);
    }
}
