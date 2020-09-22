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

import com.sun.tools.javac.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class Consumer {

    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "demo";
    private static final int CONSUMER_SLEEP_DURATION = 2000;
    private static final int NUMBER_OF_THREADS = 8;
    private static final int MAX_POLL_RECORDS = 5;
    private static final Duration POLL_DURATION = Duration.ofMillis(250);

    public static void main(String[] args) {
        final ExecutorService threadPool = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down");
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(MAX_POLL_RECORDS * CONSUMER_SLEEP_DURATION + 1000, TimeUnit.MILLISECONDS);
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
        private final KafkaConsumer<Long, String> consumer;
        private final Supplier<Boolean> isRunning;
        public WorkerThread(int id, Supplier<Boolean> isRunning) {
            this.id = id;
            this.isRunning = isRunning;
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer" + id);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            this.consumer = new KafkaConsumer<>(props);
        }
        public void run() {
            System.out.printf("Worker Thread %d started%n", id);
            consumer.subscribe(List.of(KAFKA_TOPIC));
            while (isRunning.get()) {
                ConsumerRecords<Long, String> records = consumer.poll(POLL_DURATION);
                if (records.isEmpty()) {
                    continue;
                }
                System.out.printf("Worker Thread %d received %d records%n", id, records.count());
                for (ConsumerRecord<Long, String> record : records) {
                    System.out.printf("Worker Thread %d processed \"%s\" (Offset=%d, Partition=%d)%n",
                            id, record.value(), record.offset(), record.partition());
                    try {
                        // Simulate some amount of time elapsing while processing the data
                        Thread.sleep(CONSUMER_SLEEP_DURATION);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        break;
                    }
                }
                consumer.commitAsync();
            }
            consumer.close();
            System.out.printf("Worker Thread %d stopped%n", id);
        }
    }

}
