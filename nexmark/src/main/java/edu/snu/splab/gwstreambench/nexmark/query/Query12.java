package edu.snu.splab.gwstreambench.nexmark.query;

import edu.snu.splab.gwstreambench.nexmark.model.Event;
import edu.snu.splab.gwstreambench.nexmark.model.Person;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class Query12 implements QueryBuilder {
    @Override
    public void build(final StreamExecutionEnvironment env, final ParameterTool params,
                      final Properties properties) {
        final TypeInformation<Event> typeInformation = TypeExtractor.createTypeInfo(Event.class);
        final DataStream<Event> events = env.addSource(
                new FlinkKafkaConsumer011<>("nexmarkinput",
                        new TypeInformationSerializationSchema<>(typeInformation, env.getConfig()), properties));
        events.filter((FilterFunction<Event>) event -> event.eventType == Event.EventType.PERSON)
                .map((MapFunction<Event, Person>) event -> event.person)
                .map((MapFunction<Person, String>) person -> person.name)
                .addSink(new FlinkKafkaProducer011<>("result", new SimpleStringSchema(), properties));
    }
}
