package rassus.lab2;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
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
import rassus.lab2.network.EmulatedSystemClock;
import rassus.lab2.network.SimpleSimulatedDatagramSocket;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.time.Duration;
import java.util.*;

import static java.lang.System.exit;

public class Node {
    private static String TOPIC0 = "Command";
    private static String TOPIC1 = "Register";
    private static volatile boolean stop = false;
    private static volatile HashMap<String, String> notAckMessages = new HashMap<>();
    private static volatile HashSet<JSONObject> allReadings = new HashSet<>();
    private static volatile HashSet<JSONObject> fiveSecReadings = new HashSet<>();
    private static EmulatedSystemClock scalarTime;
    private static HashMap<String, Integer> vectorTime;
    private static String id;
    private static String address = "localhost";
    private static String udpPort;
    private static ArrayList<JSONObject> otherNodesInfo = new ArrayList<>();

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis() / 1000;
        boolean startFlag = false;

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
        while(!startFlag) {
            // poll for records
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

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
        }

        //initialize emulated system clock (node scalar time)
        scalarTime = new EmulatedSystemClock();

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

        // get registration messages from kafka topic
        ConsumerRecords<String, String> otherNodes = consumer.poll(Duration.ofMillis(5000));

        for(ConsumerRecord<String, String> otherNode : otherNodes) {
            // print record details
            System.out.printf("Consumer record - topic: %s, partition: %s, offset: %d, key: %s\n",
                    otherNode.topic(), otherNode.partition(), otherNode.offset(), otherNode.key());

            // parse record value from register topic to json
            if(otherNode.topic().equals(TOPIC1)) {
                JSONObject otherNodeInfo = new JSONObject(otherNode.value());
                otherNodesInfo.add(otherNodeInfo);
            }

            // parse record value from command topic to json
            if(otherNode.topic().equals(TOPIC0)) {
                JSONObject command = new JSONObject(record.value());

                // check if node received stop command
                if(command.get("command").toString().toUpperCase().equals("STOP")) {
                    System.out.println("Received STOP command. Stopping node.");
                    stop = true;
                }
            }
        }

        // initialize node vector time
        vectorTime = new HashMap<>();
        vectorTime.put(id, 0);
        for(JSONObject otherNodeInfo : otherNodesInfo) {
            vectorTime.put(otherNodeInfo.getString("id"), 0);
        }

        // load readings
        ArrayList<String> no2Readings = new ArrayList<>();
        try(Reader csvReader = new FileReader("resources/readings.csv")) {
            Iterable<CSVRecord> csvRecords = CSVFormat.DEFAULT.parse(csvReader);
            for(CSVRecord csvRecord : csvRecords) {
                no2Readings.add(csvRecord.get("NO2"));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            exit(1);
        }

        // open udp sockets
        try {
            DatagramSocket serverSocket = new SimpleSimulatedDatagramSocket(Integer.parseInt(udpPort), 0.3, 1000);
            DatagramSocket clientSocket = new SimpleSimulatedDatagramSocket(0.3, 1000);
        }
        catch (Exception e) {
            e.printStackTrace();
            exit(1);
        }



    }

    /**
     * This class is used to continuously check if node received stop command.
     * When stop command is received, this class sets stop flag and shuts down itself, udp server and udp client threads.
     */
    public static class StopCommandConsumer implements Runnable {
        private Consumer<String, String> consumer;

        public StopCommandConsumer(Consumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            while(!stop) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record : consumerRecords) {
                    // print record details
                    System.out.printf("StopCommandConsumer: Consumer record - topic: %s, partition: %s, offset: %d, key: %s\n",
                            record.topic(), record.partition(), record.offset(), record.key());

                    // parse record value to json object
                    if(record.topic().equals(TOPIC0)) {
                        JSONObject command = new JSONObject(record.value());

                        // check if received command is stop
                        if(command.get("command").toString().toUpperCase().equals("STOP")) {
                            System.out.println("StopCommandConsumer: Received STOP command. Stopping node.");
                            stop = true;
                            break;
                        }
                    }
                }
            }
        }
    }

    public static class UDPServer implements Runnable {
        private DatagramSocket socket;

        public UDPServer(SimpleSimulatedDatagramSocket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            byte[] receiveBuf = new byte[2048];
            byte[] sendBuf;
            String receiveStr;

            while(!stop) {
                // receive packet
                DatagramPacket receivedPacket = new DatagramPacket(receiveBuf, receiveBuf.length);
                try {
                    socket.receive(receivedPacket);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    continue;
                }

                // parse received packet to json
                receiveStr = new String(receivedPacket.getData(), receivedPacket.getOffset(),
                        receivedPacket.getLength());
                JSONObject message = new JSONObject(receiveStr);

                // check message type
                if(message.getString("type").equalsIgnoreCase("ack")) {
                    // update times
                    updateTimesReceive(message);

                    // remove message with received message id from map with not ack messages
                    notAckMessages.remove(message.getString("messageId"));

                }
                else if(message.getString("type").equalsIgnoreCase("reading")) {
                    // update times
                    updateTimesReceive(message);

                    // parse it to json and save to set with readings
                    JSONObject reading = new JSONObject();
                    reading.put("scalarTime", message.getString("scalarTime"));
                    reading.put("vectorTime", new JSONObject(message.getJSONObject("vectorTime")));
                    reading.put("no2Reading", message.getString("no2Reading"));
                    fiveSecReadings.add(reading);
                    allReadings.add(reading);

                    // increase vector time for ack message
                    updateVectorTimeSend();

                    // create ack message
                    JSONObject ackMessage = new JSONObject();
                    ackMessage.put("type", "ack");
                    ackMessage.put("scalarTime", scalarTime.currentTimeMillis());
                    ackMessage.put("vectorTime", new JSONObject(vectorTime));
                    ackMessage.put("messageId", message.getString("messageId"));

                    // send ack message
                    sendBuf = ackMessage.toString().getBytes();
                    DatagramPacket sendPacket = new DatagramPacket(sendBuf,
                            sendBuf.length, receivedPacket.getAddress(), receivedPacket.getPort());
                }
            }
        }

        private void updateTimesReceive(JSONObject message) {
            // update scalar time
            long messageScalarTime = message.getLong("scalarTime");
            scalarTime.update(messageScalarTime);

            // update vector time
            // increase vector time for this node
            incrementVectorTime();

            // increase vector time for other nodes
            for (JSONObject otherNodeInfo : otherNodesInfo) {
                // compare saved vector time and received vector time
                String otherNodeId = otherNodeInfo.getString("id");
                int receivedVectorTime = message.getJSONObject("vectorTime").getInt(otherNodeId);
                int savedVectorTime = vectorTime.get(otherNodeId);

                // if received vector time is greater save it
                if (receivedVectorTime > savedVectorTime) {
                    vectorTime.put(otherNodeId, receivedVectorTime);
                }
            }
        }

        private void updateVectorTimeSend() {
            incrementVectorTime();
        }
    }

    public static class UDPClient implements Runnable {
        private DatagramSocket socket;

        public UDPClient(SimpleSimulatedDatagramSocket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {

        }
    }

    public static synchronized void incrementVectorTime() {
        // increment vector time for this node
        if(vectorTime != null && id != null) {
            vectorTime.put(id, vectorTime.get(id) + 1);
        }
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

    public static String getReading(ArrayList<String> no2Reading, long startTime) {
        int index = (int) ((System.currentTimeMillis() / 1000 - startTime) % 100);
        return no2Reading.get(index);
    }
}
