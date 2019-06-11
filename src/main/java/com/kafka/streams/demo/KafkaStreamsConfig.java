/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kafka.streams.demo;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 *
 * @author milos
 */
@Component
public class KafkaStreamsConfig {
    
    @Value("${kafka.streams.bootstrap.servers}")
    private String bootstrapServers;
    @Value("${kafka.streams.clientID}")
    private String clientId;
    @Value("${kafka.streams.threads.number}")
    private String threadNumber;
    
    public Properties createStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, clientId);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, threadNumber);
        return props;
    }
}
