package rassus.lab2;

import java.util.Scanner;

public class Node {
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
