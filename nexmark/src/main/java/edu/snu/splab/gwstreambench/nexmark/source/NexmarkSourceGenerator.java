package edu.snu.splab.gwstreambench.nexmark.source;

import edu.snu.splab.gwstreambench.nexmark.model.Auction;
import edu.snu.splab.gwstreambench.nexmark.model.Bid;
import edu.snu.splab.gwstreambench.nexmark.model.Event;
import edu.snu.splab.gwstreambench.nexmark.model.Person;

import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public final class NexmarkSourceGenerator implements Iterator<Event> {
    private static final int NUM_GENERATORS = 1;
    private static final int PERSON_PROPORTION = 1;
    private static final int AUCTION_PROPORTION = 3;
    private static final int BID_PROPORTION = 46;
    private static final int PROPORTION_DENOMINATOR =
            PERSON_PROPORTION + AUCTION_PROPORTION + BID_PROPORTION;
    private static final long EVENTS_PER_SECOND = 10000;
    private static final long INTER_EVENT_DELAY_US
            = ((1_000_000L + EVENTS_PER_SECOND / 2) / EVENTS_PER_SECOND) * NUM_GENERATORS;
    private static final long BASE_TIME = Instant.parse("2015-07-15T00:00:00.000Z").toEpochMilli();


    private long numGeneratedEvents = 0;

    @Override
    public boolean hasNext() {
        // Always has next (streaming!)
        return true;
    }

    @Override
    public Event next() {
        final long eventId = numGeneratedEvents * 954;
        final long eventTimeStamp = BASE_TIME + (eventId * INTER_EVENT_DELAY_US) / 1000L;
        final long watermark = BASE_TIME + (numGeneratedEvents * INTER_EVENT_DELAY_US) / 1000L;

        final Random random = new Random(eventId);
        final long rem = eventId % PROPORTION_DENOMINATOR;

        final Event event;
        if (rem < PERSON_PROPORTION) {
            event =
                    new Event(nextPerson(eventId, eventTimeStamp, random), eventTimeStamp, watermark);
        } else if (rem < PERSON_PROPORTION + AUCTION_PROPORTION) {
            event =
                    new Event(
                            nextAuction(numGeneratedEvents, eventId, eventTimeStamp, random), eventTimeStamp, watermark);
        } else {
            event = new Event(nextBid(eventId, eventTimeStamp, random), eventTimeStamp, watermark);
        }

        numGeneratedEvents++;
        return event;
    }


    private static final int MIN_STRING_LENGTH = 3;
    private static String nextString(final Random random, final int maxLength) {
        int len = MIN_STRING_LENGTH + random.nextInt(maxLength - MIN_STRING_LENGTH);
        final StringBuilder sb = new StringBuilder();
        while (len-- > 0) {
            if (random.nextInt(13) == 0) {
                sb.append(' ');
            } else {
                sb.append((char) ('a' + random.nextInt(26)));
            }
        }
        return sb.toString().trim();
    }
    private static String nextExactString(final Random random, int length) {
        final StringBuilder sb = new StringBuilder();
        int rnd = 0;
        int n = 0; // number of random characters left in rnd
        while (length-- > 0) {
            if (n == 0) {
                rnd = random.nextInt();
                n = 6; // log_26(2^31)
            }
            sb.append((char) ('a' + rnd % 26));
            rnd /= 26;
            n--;
        }
        return sb.toString();
    }
    private static String nextExtra(final Random random, final int currentSize, int desiredAverageSize) {
        if (currentSize > desiredAverageSize) {
            return "";
        }
        desiredAverageSize -= currentSize;
        final int delta = (int) Math.round(desiredAverageSize * 0.2);
        final int minSize = desiredAverageSize - delta;
        final int desiredSize = minSize + (delta == 0 ? 0 : random.nextInt(2 * delta));
        return nextExactString(random, desiredSize);
    }
    private static long nextLong(Random random, long n) {
        if (n < Integer.MAX_VALUE) {
            return random.nextInt((int) n);
        } else {
            return Math.abs(random.nextLong() % n);
        }
    }

    private static final long FIRST_PERSON_ID = 1000L;
    private static final int NUM_ACTIVE_PEOPLE = 1000;
    private static final int PERSON_ID_LEAD = 10;
    private static int AVG_PERSON_SIZE_BYTE = 200;
    private static final List<String> US_STATES = Arrays.asList("AZ,CA,ID,OR,WA,WY".split(","));
    private static final List<String> US_CITIES =
            Arrays.asList(
                    "Phoenix,Los Angeles,San Francisco,Boise,Portland,Bend,Redmond,Seattle,Kent,Cheyenne"
                            .split(","));
    private static final List<String> FIRST_NAMES =
            Arrays.asList("Peter,Paul,Luke,John,Saul,Vicky,Kate,Julie,Sarah,Deiter,Walter".split(","));
    private static final List<String> LAST_NAMES =
            Arrays.asList("Shultz,Abrams,Spencer,White,Bartels,Walton,Smith,Jones,Noris".split(","));
    private static long lastBase0PersonId(long eventId) {
        final long epoch = eventId / PROPORTION_DENOMINATOR;
        long offset = eventId % PROPORTION_DENOMINATOR;
        if (offset >= PERSON_PROPORTION) {
            offset = PERSON_PROPORTION - 1;
        }
        return epoch * PERSON_PROPORTION + offset;
    }
    private static long nextBase0PersonId(final long eventId, final Random random) {
        final long numPeople = lastBase0PersonId(eventId) + 1;
        final long activePeople = Math.min(numPeople, NUM_ACTIVE_PEOPLE);
        final long n = nextLong(random, activePeople + PERSON_ID_LEAD);
        return numPeople - activePeople + n;
    }
    private static String nextUSState(Random random) {
        return US_STATES.get(random.nextInt(US_STATES.size()));
    }

    private static String nextUSCity(Random random) {
        return US_CITIES.get(random.nextInt(US_CITIES.size()));
    }
    private static String nextPersonName(Random random) {
        return FIRST_NAMES.get(random.nextInt(FIRST_NAMES.size()))
                + " "
                + LAST_NAMES.get(random.nextInt(LAST_NAMES.size()));
    }
    private static String nextEmail(Random random) {
        return nextString(random, 7) + "@" + nextString(random, 5) + ".com";
    }
    private static String nextCreditCard(Random random) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(String.format("%04d", random.nextInt(10000)));
        }
        return sb.toString();
    }
    private static Person nextPerson(final long eventId, final long eventTimeStamp, final Random random) {
        final long id = lastBase0PersonId(eventId) + FIRST_PERSON_ID;
        final String name = nextPersonName(random);
        final String email = nextEmail(random);
        final String creditCard = nextCreditCard(random);
        final String city = nextUSCity(random);
        final String state = nextUSState(random);
        final int currentSize =
                8 + name.length() + email.length() + creditCard.length() + city.length() + state.length();
        final String extra = nextExtra(random, currentSize, AVG_PERSON_SIZE_BYTE);
        final Person person = new Person();
        person.id = id;
        person.name = name;
        person.emailAddress = email;
        person.creditCard = creditCard;
        person.city = city;
        person.state = state;
        person.dateTime = eventTimeStamp;
        person.extra = extra;
        return person;
    }

    private static final int NUM_CATEGORIES = 5;
    private static final int AUCTION_ID_LEAD = 10;
    private static final int HOT_SELLER_RATIO = 100;
    private static final int NUM_IN_FLIGHT_AUCTIONS = 100;
    private static final long FIRST_AUCTION_ID = 1000L;
    private static final int CONFIG_HOT_SELLER_RATIO = 4;
    private static final long FIRST_CATEGORY_ID = 10L;
    private static final int AVG_AUCTION_BYTE_SIZE = 500;
    public static long lastBase0AuctionId(final long eventId) {
        long epoch = eventId / PROPORTION_DENOMINATOR;
        long offset = eventId % PROPORTION_DENOMINATOR;
        if (offset < PERSON_PROPORTION) {
            // About to generate a person.
            // Go back to the last auction in the last epoch.
            epoch--;
            offset = AUCTION_PROPORTION - 1;
        } else if (offset >= PERSON_PROPORTION + AUCTION_PROPORTION) {
            // About to generate a bid.
            // Go back to the last auction generated in this epoch.
            offset = AUCTION_PROPORTION - 1;
        } else {
            // About to generate an auction.
            offset -= PERSON_PROPORTION;
        }
        return epoch * AUCTION_PROPORTION + offset;
    }
    public static long nextBase0AuctionId(final long nextEventId, final Random random) {
        final long minAuction =
                Math.max(lastBase0AuctionId(nextEventId) - NUM_IN_FLIGHT_AUCTIONS, 0);
        final long maxAuction = lastBase0AuctionId(nextEventId);
        return minAuction + nextLong(random, maxAuction - minAuction + 1 + AUCTION_ID_LEAD);
    }
    private static long nextAuctionLengthMs(final long currentEventNumber, final long eventsCountSoFar,
                                            final Random random, final long timestamp) {
        final long numEventsForAuctions =
                ((long) NUM_IN_FLIGHT_AUCTIONS * PROPORTION_DENOMINATOR) / AUCTION_PROPORTION;
        final long futureAuction =
                BASE_TIME+ ((currentEventNumber + numEventsForAuctions) * INTER_EVENT_DELAY_US) / 1000L;
        final long horizonMs = futureAuction - timestamp;
        return 1L + nextLong(random, Math.max(horizonMs * 2, 1L));
    }
    private static long nextPrice(final Random random) {
        return Math.round(Math.pow(10.0, random.nextDouble() * 6.0) * 100.0);
    }
    private Auction nextAuction(final long numGeneratedEvents, final long eventId,
                                final long eventTimeStamp, final Random random) {

        final long id = lastBase0AuctionId(eventId) + FIRST_AUCTION_ID;
        long seller;
        // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
        if (random.nextInt(CONFIG_HOT_SELLER_RATIO) > 0) {
            // Choose the first person in the batch of last HOT_SELLER_RATIO people.
            seller = (lastBase0PersonId(eventId) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
        } else {
            seller = nextBase0PersonId(eventId, random);
        }
        seller += FIRST_PERSON_ID;
        final long category = FIRST_CATEGORY_ID + random.nextInt(NUM_CATEGORIES);
        final long initialBid = nextPrice(random);
        final long expires = eventTimeStamp + nextAuctionLengthMs(eventId, numGeneratedEvents, random, eventTimeStamp);
        final String name = nextString(random, 20);
        final String desc = nextString(random, 100);
        final long reserve = initialBid + nextPrice(random);
        final int currentSize = 8 + name.length() + desc.length() + 8 + 8 + 8 + 8 + 8;
        final String extra = nextExtra(random, currentSize, AVG_AUCTION_BYTE_SIZE);
        final Auction auction = new Auction();
        auction.id = id;
        auction.itemName = name;
        auction.description = desc;
        auction.initialBid = initialBid;
        auction.reserve = reserve;
        auction.dateTime = eventTimeStamp;
        auction.expires = expires;
        auction.seller = seller;
        auction.category = category;
        auction.extra = extra;
        return auction;
    }

    private static final int HOT_AUCTION_RATIO = 100;
    private static final int HOT_BIDDER_RATIO = 100;
    private static final int CONFIG_HOT_AUCTION_RATIO = 2;
    private static final int CONFIG_HOT_BIDDERS_RATIO = 4;
    private static final int AVG_BID_BYTE_SIZE = 100;
    private Bid nextBid(final long eventId, final long eventTimeStamp, final Random random) {
        long auction;
        if (random.nextInt(CONFIG_HOT_AUCTION_RATIO) > 0) {
            // Choose the first auction in the batch of last HOT_AUCTION_RATIO auctions.
            auction = (lastBase0AuctionId(eventId) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO;
        } else {
            auction = nextBase0AuctionId(eventId, random);
        }
        auction += FIRST_AUCTION_ID;

        long bidder;
        if (random.nextInt(CONFIG_HOT_BIDDERS_RATIO) > 0) {
            bidder = (lastBase0PersonId(eventId) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1;
        } else {
            bidder = nextBase0PersonId(eventId, random);
        }
        bidder += FIRST_PERSON_ID;

        long price = nextPrice(random);
        int currentSize = 8 + 8 + 8 + 8;
        String extra = nextExtra(random, currentSize, AVG_BID_BYTE_SIZE);
        final Bid bid = new Bid();
        bid.auction = auction;
        bid.bidder = bidder;
        bid.price = price;
        bid.dateTime = eventTimeStamp;
        bid.extra = extra;
        return bid;
    }
}
