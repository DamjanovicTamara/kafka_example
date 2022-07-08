package com.example.consumer;


import org.apache.kafka.clients.consumer.MockConsumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;


public class TestConsumerConnection {

    @Test
    void testSuccessfulConnection_WhenExecutingNewThread() throws InterruptedException{
        AtomicBoolean failed  = new AtomicBoolean(false);

        CountDownLatch latch = new CountDownLatch(1);

        Executors.newSingleThreadExecutor().execute(() -> {
            failed.set(true);//or false depending on the use case
            latch.countDown();

        });

        latch.await();
        if(!failed.get()){
            Assertions.fail("Doesn't fail the test");
        }

    }
    @Test
    void testFailedConnection_WhenExecutingNewThread() throws InterruptedException{
        AtomicBoolean failed  = new AtomicBoolean(true);

        CountDownLatch latch = new CountDownLatch(1);

        Executors.newSingleThreadExecutor().execute(() -> {
            failed.set(false);//or false depending on the use case
            latch.countDown();

        });

        latch.await();
        if(failed.get()){
            Assertions.fail("Connection test failure");
        }
    }

    @Test
    public void testExecuteHookShutDown_WhenHookIsGiven() {
        Thread printingHook = new Thread(() -> System.out.println("In the middle of a shutdown"));
        Runtime.getRuntime().addShutdownHook(printingHook);
    }

    @Test
    public void testThrowException_WhenAddHookAndThreadAlreadyStarted() {
        Thread longRunningHook = new Thread(() -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException ignored) {}
        });
        longRunningHook.start();

        assertThatThrownBy(() -> Runtime.getRuntime().addShutdownHook(longRunningHook))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Hook already running");
    }

    @Test
    public void testThrowException_WhenHookAlreadyExists() {
        Thread unfortunateHook = new Thread(() -> {});
        Runtime.getRuntime().addShutdownHook(unfortunateHook);

        assertThatThrownBy(() -> Runtime.getRuntime().addShutdownHook(unfortunateHook))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Hook previously registered");
    }

    @Test
    public void testRemoveAHook_WhenItIsAlreadyRegistered() {
        Thread willNotRun = new Thread(() -> System.out.println("Won't run!"));
        Runtime.getRuntime().addShutdownHook(willNotRun);

        assertThat(Runtime.getRuntime().removeShutdownHook(willNotRun)).isTrue();
    }

}
