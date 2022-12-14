package rassus.lab2;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.util.Properties;
import java.util.Scanner;

import static java.lang.System.exit;

public class CoordinatorNode {

    private static String TOPIC = "Command";

    public static void main(String[] args) {
        // create producer
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerProperties);

        // command input
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.print("Send a command: ");
            String command = sc.nextLine().toUpperCase();

            // check if command is valid
            if(!command.equals("START") && !command.equals("STOP")) {
                System.out.println("Valid commands are:\nSTART - starts a group of nodes\nSTOP - stops and shuts down all nodes\n\n");
                continue;
            }
            System.out.println("Sending " + command + " command.");

            // create json object for command
            JSONObject jsonCommand = new JSONObject();
            jsonCommand.put("command", command);

            // create producer record for command topic
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null, jsonCommand.toString());

            // send producer record to kafka server
            producer.send(record);
            producer.flush();

            // stop coordinator node after it sent stop command
            if(!command.equals("STOP")) {
                System.out.println("Shutting down...");
                exit(0);
            }

        }
    }
}
