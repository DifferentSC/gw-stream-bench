package edu.snu.splab.gwstreambench.nexmark.model;

public class Bid {
    public long auction; // foreign key: Auction.id
    public long bidder; // foreign key: Person.id
    public long price;
    public long dateTime;
    public String extra;
}
