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
import rassus.lab2.DTO.AckMessage;
import rassus.lab2.DTO.ReadingMessage;
import rassus.lab2.network.EmulatedSystemClock;
import rassus.lab2.network.SimpleSimulatedDatagramSocket;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;

import static java.lang.System.exit;
import static java.lang.Thread.sleep;

public class Node {
    private static String TOPIC0 = "Command";
    private static String TOPIC1 = "Register";
    private static volatile boolean stop = false;
    private static volatile HashMap<MessageId, ReadingMessage> notAckMessages = new HashMap<>();
    private static volatile HashSet<Reading> allReadings = new HashSet<>();
    private static volatile HashSet<Reading> fiveSecReadings = new HashSet<>();
    private static EmulatedSystemClock scalarTime;
    private static HashMap<String, Integer> vectorTime;
    private static String id;
    private static String address = "localhost";
    private static String udpPort;
    private static ArrayList<NodeInfo> otherNodesInfo = new ArrayList<>();
    private static ArrayList<String> no2Readings = new ArrayList<>();

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
        NodeInfo registerData = new NodeInfo();
        registerData.setId(id);
        registerData.setAddress(address);
        registerData.setPort(udpPort);
        JSONObject registerDataJson = new JSONObject(registerData);

        // create producer record for registration data
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC1, null, registerDataJson.toString());

        // send producer record to kafka server
        producer.send(record);
        producer.flush();

        // get registration messages from kafka topic
        ConsumerRecords<String, String> otherNodes = consumer.poll(Duration.ofMillis(5000));

        for(ConsumerRecord<String, String> otherNode : otherNodes) {
            // print record details
            System.out.printf("Consumer record - topic: %s, partition: %s, offset: %d, key: %s\n",
                    otherNode.topic(), otherNode.partition(), otherNode.offset(), otherNode.key());

            // parse record value from register topic to NodeInfo
            if(otherNode.topic().equals(TOPIC1)) {
                JSONObject otherNodeInfoJson = new JSONObject(otherNode.value());
                NodeInfo otherNodeInfo = new NodeInfo(otherNodeInfoJson);
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
        for(NodeInfo otherNodeInfo : otherNodesInfo) {
            vectorTime.put(otherNodeInfo.getId(), 0);
        }

        // load readings
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
        DatagramSocket serverSocket = null;
        DatagramSocket clientSocket = null;
        try {
            serverSocket = new SimpleSimulatedDatagramSocket(Integer.parseInt(udpPort), 0.3, 1000);
            clientSocket = new SimpleSimulatedDatagramSocket(0.3, 1000);
        }
        catch (Exception e) {
            e.printStackTrace();
            exit(1);
        }

        // start udp server, udp client, stop command consumer and readings sorter in separate threads
        Thread[] threads = new Thread[4];
        StopCommandConsumer stopCommandConsumer = new StopCommandConsumer(consumer);
        UDPServer udpServer = new UDPServer(serverSocket);
        UDPClient udpClient = new UDPClient(clientSocket);
        ReadingsSorter readingsSorter = new ReadingsSorter();

        threads[0] = new Thread(stopCommandConsumer);
        threads[1] = new Thread(udpServer);
        threads[2] = new Thread(udpClient);
        threads[3] = new Thread(readingsSorter);

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // after stop command sort and print out all readings
        // create comparator that compares by scalar and vector time
        ScalarTimeComparator scComp = new ScalarTimeComparator();
        VectorTimeComparator vcComp = new VectorTimeComparator();
        Comparator<Reading> comp = scComp.thenComparing(vcComp);

        // create temp list that will be sorted
        List<Reading> temp = new ArrayList<>(allReadings);

        // sort temp list and print it out
        temp.sort(comp);
        temp.forEach(System.out::println);
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

        public UDPServer(DatagramSocket socket) {
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
                    // create ack message
                    AckMessage ackMessage = new AckMessage(message);

                    // update times
                    updateTimesReceive(ackMessage);

                    // remove message with received message id from map with not ack messages
                    notAckMessages.remove(ackMessage.getMessageId());

                }
                else if(message.getString("type").equalsIgnoreCase("reading")) {
                    // create reading message
                    ReadingMessage readingMessage = new ReadingMessage(message);

                    // update times
                    updateTimesReceive(readingMessage);

                    // construct reading with reading message and save to set with readings
                    Reading reading = new Reading(readingMessage);
                    fiveSecReadings.add(reading);
                    allReadings.add(reading);

                    // increase vector time for ack message
                    updateVectorTimeSend();

                    // create ack message
                    AckMessage ackMessage = new AckMessage();
                    ackMessage.setScalarTime(scalarTime.currentTimeMillis());
                    ackMessage.setVectorTime(vectorTime);
                    ackMessage.setMessageId(readingMessage.getMessageId());

                    // send ack message
                    JSONObject ackMessageJson = new JSONObject(ackMessage);
                    sendBuf = ackMessageJson.toString().getBytes();
                    DatagramPacket sendPacket = new DatagramPacket(sendBuf,
                            sendBuf.length, receivedPacket.getAddress(), receivedPacket.getPort());
                }
            }
        }

        private void updateTimesReceive(Message message) {
            // update scalar time
            long messageScalarTime = message.getScalarTime();
            scalarTime.update(messageScalarTime);

            // update vector time
            // increase vector time for this node
            incrementVectorTime();

            // increase vector time for other nodes
            for (NodeInfo otherNodeInfo : otherNodesInfo) {
                // compare saved vector time and received vector time
                String otherNodeId = otherNodeInfo.getId();
                int receivedVectorTime = message.getVectorTime().get(otherNodeId);
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
        private long messageCounter;

        public UDPClient(DatagramSocket socket) {
            this.socket = socket;
            messageCounter = 0;
        }

        @Override
        public void run() {
            while(!stop) {
                // get reading for sending
                Double no2Reading = Double.valueOf(getReading());

                // build reading
                Reading reading = new Reading();
                reading.setScalarTime(scalarTime.currentTimeMillis());
                reading.setVectorTime(vectorTime);
                reading.setNo2Reading(no2Reading);

                // add the reading to readings adn 5 second readings
                allReadings.add(reading);
                fiveSecReadings.add(reading);

                // send message to all other nodes
                for(NodeInfo otherNodeInfo : otherNodesInfo) {
                    // create destination address for node
                    InetAddress destAddress = null;
                    try {
                        destAddress = InetAddress.getByName(otherNodeInfo.getAddress());
                    } catch (UnknownHostException e) {
                        System.out.println("UDP Client: unknown address for node " + otherNodeInfo.getId());
                    }

                    // increment vector time before sending
                    incrementVectorTime();

                    // build message for sending
                    ReadingMessage message = new ReadingMessage();
                    message.setScalarTime(scalarTime.currentTimeMillis());
                    message.setVectorTime(vectorTime);
                    message.setNo2Reading(no2Reading);
                    message.setNodeId(otherNodeInfo.getId());
                    message.setMessageId(new MessageId(messageCounter, otherNodeInfo.getId()));

                    // prepare buffer for sending
                    JSONObject messageJson = new JSONObject(message);
                    byte[] sendBuf = messageJson.toString().getBytes();

                    // create a datagram packet for sending data
                    DatagramPacket packet = new DatagramPacket(sendBuf, sendBuf.length,
                            destAddress, Integer.parseInt(otherNodeInfo.getPort()));

                    // send a datagram packet from this socket
                    try {
                        socket.send(packet);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    // add message to not ack
                    notAckMessages.put(message.getMessageId(), message);
                }

                // resend not ack messages
                notAckMessages.forEach((k, v) -> {
                    // for messages not sent in this cycle
                    if(k.getMessageNumber() != messageCounter) {
                        // get node info
                        NodeInfo otherNodeInfo = null;
                        for(NodeInfo temp : otherNodesInfo) {
                            if(temp.getId().equalsIgnoreCase(v.getNodeId())) {
                                otherNodeInfo = temp;
                                break;
                            }
                        }
                        if(otherNodeInfo == null) return;

                        // create destination address for node
                        InetAddress destAddress = null;
                        try {
                            destAddress = InetAddress.getByName(otherNodeInfo.getAddress());
                        } catch (UnknownHostException e) {
                            System.out.println("UDP Client: unknown address for node " + otherNodeInfo.getId());
                        }

                        // increment vector time before sending
                        incrementVectorTime();

                        // prepare buffer for sending
                        JSONObject messageJson = new JSONObject(v);
                        byte[] sendBuf = messageJson.toString().getBytes();

                        // create a datagram packet for sending data
                        DatagramPacket packet = new DatagramPacket(sendBuf, sendBuf.length,
                                destAddress, Integer.parseInt(otherNodeInfo.getPort()));

                        // send a datagram packet from this socket
                        try {
                            socket.send(packet);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

                // increase message counter
                messageCounter++;

                // wait one second
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static class ReadingsSorter implements Runnable {
        @Override
        public void run() {
            while(!stop) {
                // create comparator that compares by scalar and vector time
                ScalarTimeComparator scComp = new ScalarTimeComparator();
                VectorTimeComparator vcComp = new VectorTimeComparator();
                Comparator<Reading> comp = scComp.thenComparing(vcComp);

                // create temp list that will be sorted
                List<Reading> temp = new ArrayList<>(fiveSecReadings);

                // empty five sec readings for next batch of readings
                fiveSecReadings = new HashSet<>();

                // sort temp list and print it out
                temp.sort(comp);
                temp.forEach(System.out::println);

                // calculate average reading and print it out
                System.out.println("Average no2 reading: " + temp.stream().mapToDouble(Reading::getNo2Reading).average());

                // sleep for 5 seconds
                try {
                    sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
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

    public static String getReading() {
        int index = (int) ((scalarTime.currentTimeMillis() / 1000) % 100);
        return no2Readings.get(index);
    }
}
