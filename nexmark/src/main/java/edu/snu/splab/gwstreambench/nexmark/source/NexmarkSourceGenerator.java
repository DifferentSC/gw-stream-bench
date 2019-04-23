package edu.snu.splab.gwstreambench.nexmark.source;

import edu.snu.splab.gwstreambench.nexmark.model.Auction;
import edu.snu.splab.gwstreambench.nexmark.model.Bid;
import edu.snu.splab.gwstreambench.nexmark.model.Event;
import edu.snu.splab.gwstreambench.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.sources.generator.Generator;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.ByteArrayDataOutputView;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Iterator;

public final class NexmarkSourceGenerator implements Iterator<byte[]> {
    private final byte[][] events;
    private int nextEventIdx = 0;

    public NexmarkSourceGenerator(final int eventsPerSec) throws IOException {
        final int numEvents = eventsPerSec * 11;
        final NexmarkConfiguration conf = NexmarkConfiguration.DEFAULT.copy();
        conf.numEvents = numEvents;
        conf.firstEventRate = eventsPerSec;
        conf.nextEventRate = eventsPerSec;
        final Generator generator = new Generator(new GeneratorConfig(conf, System.currentTimeMillis(), 0, 0L, 0));
        final ByteArrayDataOutputView dataOutputView = new ByteArrayDataOutputView(1000);
        final TypeInformation<Event> eventTypeInfo = TypeExtractor.createTypeInfo(Event.class);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.getConfig().disableGenericTypes();
        final TypeSerializer<Event> serializer = eventTypeInfo.createSerializer(env.getConfig());
        events = new byte[numEvents][];
        for (int i = 0; i < numEvents; i++) {
            serializer.serialize(nextEvent(generator), dataOutputView);
            events[i] = dataOutputView.toByteArray();
            dataOutputView.reset();
        }
    }

    @Override
    public boolean hasNext() {
        // Always has next (streaming!)
        return true;
    }

    @Override
    public byte[] next() {
        return events[nextEventIdx++];
    }

    public static Event nextEvent(final Generator generator) {
        final TimestampedValue<org.apache.beam.sdk.nexmark.model.Event> timestampedValue = generator.next();
        final org.apache.beam.sdk.nexmark.model.Event nexmarkEvent = timestampedValue.getValue();
        final Event event = new Event();
        event.systemTimeStamp = System.currentTimeMillis();
        event.auction = nexmarkEvent.newAuction == null ? null : convert(nexmarkEvent.newAuction);
        event.bid = nexmarkEvent.bid == null ? null : convert(nexmarkEvent.bid);
        event.person = nexmarkEvent.newPerson == null ? null : convert(nexmarkEvent.newPerson);
        event.eventType = event.auction != null ? Event.EventType.AUCTION :
                event.bid != null ? Event.EventType.BID :
                        event.person != null ? Event.EventType.PERSON : null;
        event.timestamp = timestampedValue.getTimestamp().getMillis();
        return event;
    }

    private static Auction convert(org.apache.beam.sdk.nexmark.model.Auction in) {
        final Auction out = new Auction();
        out.id = in.id;
        out.itemName = in.itemName;
        out.description = in.description;
        out.initialBid = in.initialBid;
        out.reserve = in.reserve;
        out.dateTime = in.dateTime.getMillis();
        out.expires = in.expires.getMillis();
        out.seller = in.seller;
        out.category = in.category;
        out.extra = in.extra;
        return out;
    }

    private static Bid convert(org.apache.beam.sdk.nexmark.model.Bid in) {
        final Bid out = new Bid();
        out.auction = in.auction;
        out.bidder = in.bidder;
        out.price = in.price;
        out.dateTime = in.dateTime.getMillis();
        out.extra = in.extra;
        return out;
    }

    private static Person convert(org.apache.beam.sdk.nexmark.model.Person in) {
        final Person out = new Person();
        out.id = in.id;
        out.name = in.name;
        out.emailAddress = in.emailAddress;
        out.creditCard = in.creditCard;
        out.city = in.city;
        out.state = in.state;
        out.dateTime = in.dateTime.getMillis();
        out.extra = in.extra;
        return out;
    }
}
