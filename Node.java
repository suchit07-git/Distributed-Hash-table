import java.net.InetSocketAddress;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

public class Node {

    private long id;
    private InetSocketAddress address;
    private InetSocketAddress predecessor;
    private HashMap<Integer, InetSocketAddress> fingerTable;

    public Node(InetSocketAddress address) { 
        this.address = address;
        try { 
            id = hashAddress(address);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 algorithm not found");
        }
        fingerTable = new HashMap<>();
        predecessor = null;
        System.out.println("Node initialized with IP address: " + address + ", ID: " + id + ", Predecessor: " + predecessor);
    }

    private static long hashAddress(InetSocketAddress address) throws NoSuchAlgorithmException {
        String nodeInfo = address.getAddress() + ":" + address.getPort();
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] hashBytes = sha1.digest(nodeInfo.getBytes(StandardCharsets.UTF_8));
        BigInteger nodeId = new BigInteger(1, hashBytes);
        int m = 32;
        BigInteger chordSpace = BigInteger.TWO.pow(m);
        return nodeId.mod(chordSpace).longValue();
    }


    public boolean join(InetSocketAddress bootstrapNode) {
        return true;
    }

    public void printNeighbours() {

    }

    public long getID() {
        return id;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public InetSocketAddress getPredecessor() {
        return predecessor;
    }

    public InetSocketAddress getSuccessor() {
        if (fingerTable.size() > 0 && fingerTable.containsKey(1))
            return fingerTable.get(1);
        return null;
    }
}

