package rassus.lab2;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class Node {
    private static String TOPIC0 = "Command";
    private static String TOPIC1 = "Register";
    public static void main(String[] args) {
        String id;
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
