package edu.snu.splab.gwstreambench.nexmark.model;

import java.util.Date;

public class Bid {
    public long auction; // foreign key: Auction.id
    public long bidder; // foreign key: Person.id
    public long price;
    public Date dateTime;
    public String extra;
}
