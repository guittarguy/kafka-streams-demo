/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kafka.streams.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * @author milos
 */
@Service
public class StreamsService {

    @Autowired
    private KafkaStreamsConfig config;

    public static final String TOPIC = "test";
    public static final Logger LOGGER = Logger.getAnonymousLogger();

    final Long inactivityGap = TimeUnit.MINUTES.toMillis(1);

    public void startUp() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.Long(), Serdes.String()));
//
//        stream.foreach((Long key, String value) -> {
//            LOGGER.log(Level.INFO, "{0} -> {1}", new Object[]{key, value});
//        });
//
        stream.groupByKey(Grouped.keySerde(Serdes.Long())).aggregate(() -> "0",
                (aggKey, newValue, aggValue) -> {
                    Long value = Long.valueOf((String)aggValue);
                    value++;
                    return value.toString();
                },
                Materialized.as("aggregated-stream-store")).toStream().foreach((k, v) -> {
            LOGGER.log(Level.INFO, "Key {0} has {1} records", new Object[]{k, v});
        });
//        
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config.createStreamsConfig());
        streams.start();
        
    }
}
