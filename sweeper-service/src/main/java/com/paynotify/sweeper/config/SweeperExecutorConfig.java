package com.paynotify.sweeper.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class SweeperExecutorConfig {

    /**
     * Bounded pool used to sweep platforms in parallel, so one slow/down platform does not delay the
     * re-delivery of the others. Shut down with the application context.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService sweeperExecutor(SweeperProperties props) {
        return Executors.newFixedThreadPool(props.platformParallelism(),
                Thread.ofPlatform().name("sweeper-", 0).daemon(true).factory());
    }
}
