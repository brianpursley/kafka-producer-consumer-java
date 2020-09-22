/*
   Copyright 2020 Brian Pursley

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.cinlogic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class Producer {

    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "demo";
    private static final int PRODUCER_SLEEP_DURATION = 1000;
    private static final int NUMBER_OF_THREADS = 4;

    public static void main(String[] args) {
        final ExecutorService threadPool = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down");
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(PRODUCER_SLEEP_DURATION + 1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            threadPool.submit(new WorkerThread(i, () -> !threadPool.isShutdown()));
        }
    }

    private static class WorkerThread implements Runnable {
        private final int id;
        private final KafkaProducer<Long, String> producer;
        private final Supplier<Boolean> isRunning;
        public WorkerThread(int id, Supplier<Boolean> isRunning) {
            this.id = id;
            this.isRunning = isRunning;
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer" + id);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            this.producer = new KafkaProducer<>(props);
        }
        public void run() {
            System.out.printf("Worker Thread %d started%n", id);
            while (isRunning.get()) {
                try {
                    // Simulate some amount of time elapsing while generating the data to produce
                    Thread.sleep(PRODUCER_SLEEP_DURATION);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
                String value = String.format("payload %s from producer worker thread %d", UUID.randomUUID(), id);
                producer.send(new ProducerRecord<>(KAFKA_TOPIC, value));
                System.out.printf("Worker Thread %d sent \"%s\"%n", id, value);
            }
            producer.close();
            System.out.printf("Worker Thread %d stopped%n", id);
        }
    }

}
