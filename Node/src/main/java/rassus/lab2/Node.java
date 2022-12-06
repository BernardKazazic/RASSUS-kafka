package rassus.lab2;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class Node {
    private static String TOPIC0 = "Command";
    private static String TOPIC1 = "Register";
    public static void main(String[] args) {
        String id;
        String address = "localhost";
        String udpPort;
        Scanner sc = new Scanner(System.in);

        // initialize node id and port values
        System.out.println("Starting node...");
        System.out.print("Set node id: ");

        id = sc.nextLine();

        while(id.isBlank()) {
            System.out.println("Id can not be blank.");
            System.out.print("Set node id: ");
            id = sc.nextLine();
        }

        System.out.println("Valid udp ports are 3000-3999.");
        System.out.print("Set node udp port: ");

        udpPort = sc.nextLine();

        while(!isValidPort(udpPort)) {
            System.out.println("Udp port is not valid. Valid ports are 3000-3999.");
            System.out.print("Set node upd port: ");
            udpPort = sc.nextLine();
        }

        // create kafka consumer
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Consumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProperties);

        // subscribe node on topics
        consumer.subscribe(Collections.singleton(TOPIC0));
        consumer.subscribe(Collections.singleton(TOPIC1));

        // wait for start command
        while(true) {
            // poll for records
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            boolean startFlag = false;

            for(ConsumerRecord<String, String> record : consumerRecords) {
                // print record details
                System.out.printf("Consumer record - topic: %s, partition: %s, offset: %d, key: %s\n",
                        record.topic(), record.partition(), record.offset(), record.key());

                // parse record value to json object
                if(record.topic().equals(TOPIC0)) {
                    JSONObject command = new JSONObject(record.value());

                    // check if received command is start
                    if(command.get("command").toString().toUpperCase().equals("START")) {
                        System.out.println("Received START command. Starting node function.");
                        startFlag = true;
                        break;
                    }
                }
            }
            if(startFlag) break;
        }

        // create kafka producer
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerProperties);

        // create message for registration
        JSONObject registerData = new JSONObject();
        registerData.put("id", id);
        registerData.put("address", address);
        registerData.put("port", udpPort);

        // create producer record for registration data
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC1, null, registerData.toString());

        // send producer record to kafka server
        producer.send(record);
        producer.flush();
    }

    public static boolean isValidPort(String stringPort) {
        try {
            int integerPort = Integer.parseInt(stringPort);
            return integerPort >= 3000 && integerPort < 4000;
        }
        catch (NumberFormatException e) {
            return false;
        }
    }
}
