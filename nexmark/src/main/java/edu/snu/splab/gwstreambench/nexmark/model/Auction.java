package edu.snu.splab.gwstreambench.nexmark.model;

public class Auction {
    public long id; // primary key
    public String itemName;
    public String description;
    public long initialBid;
    public long reserve;
    public long dateTime;
    public long expires;
    public long seller; // foreign key: Person.id
    public long category; // foreign key: Category.id
    public String extra;
}
