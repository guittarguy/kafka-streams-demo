/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kafka.streams.demo;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
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
    
    public void startUp() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.Long(), Serdes.String()));
        stream.foreach((Long key, String value) -> {
            LOGGER.log(Level.INFO, "{0} -> {1}", new Object[]{key, value});
        });
        Topology topology = builder.build();
        LOGGER.log(Level.INFO, topology.toString());
        KafkaStreams streams = new KafkaStreams(topology, config.createStreamsConfig());
        streams.start();
    }
}
