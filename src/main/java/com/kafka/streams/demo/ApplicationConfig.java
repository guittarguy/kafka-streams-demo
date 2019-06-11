/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kafka.streams.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

/**
 *
 * @author milos
 */
@SpringBootApplication
public class ApplicationConfig {
    
    @Autowired
    private StreamsService streamsService;

    @EventListener
    public void onApplicationReady(ApplicationReadyEvent evt) {
        streamsService.startUp();
    }
}
