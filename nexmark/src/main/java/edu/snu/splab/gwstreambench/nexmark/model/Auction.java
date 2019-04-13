package edu.snu.splab.gwstreambench.nexmark.model;

import java.util.Date;

public class Auction {
    public long id; // primary key
    public String itemName;
    public String description;
    public long initialBid;
    public long reserve;
    public Date dateTime;
    public Date expires;
    public long seller; // foreign key: Person.id
    public long category; // foreign key: Category.id
    public String extra;
}
