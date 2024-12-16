import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Scanner;

public class Chord {

    private static Node node;
    private static InetSocketAddress bootstrapNode;

    public static void main(String[] args) {
        String IPAddress = null;
        try {
            IPAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        if (args.length == 0) { 
            System.out.print("Usage: ");
            System.out.println("java Chord <port_number> or");
            System.out.println("java Chord <port_number> <existing_node_ip_address> <existing_node_port_number>");
            System.exit(0);
        }

        int port = Integer.parseInt(args[0]);
        node = new Node(new InetSocketAddress(IPAddress, port));

        if (args.length == 1) {
            // Creating a ring
            bootstrapNode = node.getAddress();
        } else if (args.length == 3) {
            // Joining an existing ring
            bootstrapNode = new InetSocketAddress(args[1], Integer.parseInt(args[2]));
            if (bootstrapNode == null) {
                System.err.println("Bootstrap node address is invalid");
                System.exit(0);
            }
        } else {
            System.err.println("Wrong number of arguments specified");
            System.exit(0);
        }

        boolean isJoinSuccessful = node.join(bootstrapNode);
        if (!isJoinSuccessful) {
            System.err.println("Couldn't join the chord ring");
            System.exit(0);
        }
        System.out.println("Joining the Chord ring.");
        System.out.println("IP Address: " + IPAddress);
        node.printNeighbours();
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.println("Commands:");
            System.out.println("- get <key>");
            System.out.println("- put <key> <value>");
            System.out.println("- delete <key>");
            System.out.println("- exit");
            String command = sc.nextLine();
            if (command.startsWith("exit")) {
                System.err.println("Leaving the ring");
                sc.close();
                System.exit(0);
            } else if (command.startsWith("get")) {
            } else if (command.startsWith("put")) {
            } else if (command.startsWith("delete")) {
            } else {
                System.err.println("Invalid command, try again");
            }
        }
    }
}
