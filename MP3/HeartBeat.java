import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

// Class that deals with processing messages and crafting messages to be
// sent as HeartBeats. 
public class HeartBeat {
    private static final int NUM_INTRODUCERS = 3;

    private static final int NUM_GOSSIPS = 2;

    private static final int GOSSIP_HEARTBEAT_PERIOD = 500;
    private static final int GOSSIP_FAIL_PERIOD = 6 * GOSSIP_HEARTBEAT_PERIOD;
    private static final int GOSSIP_FAIL_BROADCAST_PERIOD = GOSSIP_FAIL_PERIOD;
    private static final int GOSSIP_CLEANUP_PERIOD = GOSSIP_FAIL_BROADCAST_PERIOD + GOSSIP_FAIL_PERIOD;

    private static final int ATA_HEARTBEAT_PERIOD = 1000;
    private static final int ATA_FAIL_PERIOD = (int) Math.round(3.5 * ATA_HEARTBEAT_PERIOD);
    private static final int ATA_FAIL_BROADCAST_PERIOD = ATA_FAIL_PERIOD;
    private static final int ATA_CLEANUP_PERIOD = ATA_FAIL_BROADCAST_PERIOD + ATA_FAIL_PERIOD;

    public static Object mutex = new Object(); // mutex for thread synchronization
    public static int self_counter = 1;
    public static boolean is_joined = false;
    public static String id = null;
    public static InetAddress ip = getIP();
    public static boolean is_gossip = false;
    public static int algorithm_counter = 0;
    public static String vm_name = MembershipList.getVMFromIp(ip);

    // Returns the IP address of the node
    private static InetAddress getIP() {
        try (final DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            InetAddress ip = InetAddress.getByName(socket.getLocalAddress().getHostAddress());
            return ip;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Gives a "grace period" to newly joined machines or when switching algorithms
    // to avoid mass failures due to altering the failure and heartbeat periods
    private static void giveGrace() {
        long currentTime = System.currentTimeMillis();
        for (MembershipList.Member mem : MembershipList.getMembers()) {
            long failTime = is_gossip ? GOSSIP_FAIL_PERIOD : ATA_FAIL_PERIOD;
            mem.lastModified = currentTime + failTime;
        }
    }

    // Changes the algorithm from gossip to all to all
    // If algorithm is already gossip, then it does nothing and prints an
    // appropriate message
    public static void changeAlgorithm(boolean changeAlg) {
        if (changeAlg == is_gossip) {
            System.out.println("NOP; Already in use");
            return;
        }
        if (!is_joined) {
            // TODO is this the best option?
            System.out.println("The system isn't running; there's no algorithm to switch");
            return;
        }
        algorithm_counter++;
        is_gossip = changeAlg;
        HeartBeat.giveGrace();
    }

    // Helper function for joining a node to the membership service
    // If the node is already "joined" then it does nothing
    // Appends to the membership list
    private static void doJoin() {
        if (is_joined) {
            System.out.println("Already active; you have to leave before joining again");
            return;
        }
        System.out.println("Joining");
        Log.log("Initiated join command");
        assert (MembershipList.getSize() == 0);
        HeartBeat.id = Long.toString(System.currentTimeMillis()) + ip.toString();
        MembershipList.addMember(ip, id);
        HeartBeat.is_joined = true;
        HeartBeat.self_counter = 1;
    }

    // Calling function for joining a node to the membership service
    public static void join() {
        doJoin();
    }

    // Helper function for making a node leave the membership service
    // If the node is already considered "left" then it does nothing
    // Sends out message in either gossip or all to all based off of what the
    // current algorithm is to properly notify other nodes that this node is leaving
    // Removes from the membership list
    // Clears failures, join, leave queue
    // Sets current Id to null
    private static void doLeave() {
        if (!is_joined) {
            System.out.println("Already inactive; you have to join before leaving");
            return;
        }
        System.out.println("Leaving...");
        Log.log("Initiated leave command");

        MembershipList.memberLeave(HeartBeat.id);

        for (int i = 0; i < 5; i++) {
            if (HeartBeat.is_gossip) {
                doGossip();
            } else {
                doATA();
            }
        }
        MembershipList.removeAll();
        MembershipList.recentFailures.clear();
        MembershipList.recentJoin.clear();
        MembershipList.recentLeaves.clear();
        HeartBeat.is_joined = false;
        HeartBeat.id = null;
        HeartBeat.self_counter = 0;
    }

    // Calling function for a node leaving the membership service
    public static void leave() {
        doLeave();
    }

    // Sends a message to the introducers and introduces the calling node
    private static void introduce() {
        String heartbeat = getHeartbeatFull();
        for (int i = 0; i < NUM_INTRODUCERS && i < MembershipList.allVMs.size(); i++) {
            Messenger.send(heartbeat, MembershipList.allVMs.get(i));
        }
    }

    // Gets the full heartbeat message
    // This consists of
    private static String getHeartbeatFull() {
        // format: f"{A or G} {algcounter} {senderID} {leavesize} {for each leave: id}
        // {failureSize} {for each failure: id} {algorithm specific message}"
        String s = is_gossip ? "G" : "A";
        s += " ";

        s += Integer.toString(algorithm_counter) + " ";
        s += id + " ";
        Queue<String> recentLeaves = MembershipList.recentLeaves;
        s += Integer.toString(recentLeaves.size()) + " ";

        for (String memberId : recentLeaves) {
            s += memberId + " ";
        }

        List<String> broadcastFailures = new ArrayList<>();
        long now = System.currentTimeMillis();
        long t_broadcast = is_gossip ? GOSSIP_FAIL_BROADCAST_PERIOD : ATA_FAIL_BROADCAST_PERIOD;
        for (String id : MembershipList.recentFailures) {
            MembershipList.Member m = MembershipList.getMember(id);
            if (now - m.lastModified < t_broadcast) {
                try{
                    broadcastFailures.add(id);
                } catch(NullPointerException e){}
                
            }
        }

        s += Integer.toString(broadcastFailures.size()) + " ";
        for (String id : broadcastFailures) {
            s += id + " ";
        }

        if (is_gossip) {
            s += getHeartbeatGossip();
        } else {
            s += getHeartbeatATA();
        }

        return s;
    }

    // Gets the heartbeat message for all-to-all style heartbeating
    // ATA format: f"{join buffer size} {for each: id} {sender HB counter}"
    private static String getHeartbeatATA() {

        Queue<String> recentJoins = MembershipList.recentJoin;
        String joinString = Integer.toString(recentJoins.size()) + " ";
        for (String memberId : recentJoins) {
            joinString += memberId + " ";
        }
        joinString += Integer.toString(self_counter);
        return joinString;
    }

    // Gets the heartbeat message for gossip style heartbeating
    // gossip message format: f"{tableSize} {for each row: {id} {counter}} "
    // (note the trailing space)
    private static String getHeartbeatGossip() {

        long currentTime = System.currentTimeMillis();

        List<MembershipList.Member> membersToSend = new ArrayList<>();
        for (MembershipList.Member member : MembershipList.getMembers()) {
            if (MembershipList.recentFailures.contains(member.id)) {
                continue;
            }
            long timeDifference = currentTime - member.lastModified;

            if (timeDifference < GOSSIP_FAIL_PERIOD && !MembershipList.recentFailures.contains(member.id)) {
                membersToSend.add(member);
            }
        }

        String gossipString = Integer.toString(membersToSend.size()) + " ";
        for (MembershipList.Member member : membersToSend) {
            gossipString += member.id + " " + Integer.toString(member.counter) + " ";
        }

        return gossipString;
    }

    // Function that implements all to all style heartbeating
    // Calling node will send heartbeats to all other heartbeats in the membership
    // service
    private static void doATA() {
        String heartbeat = getHeartbeatFull();
        long currentTime = System.currentTimeMillis();
        for (MembershipList.Member member : MembershipList.getMembers()) {
            if (member.id.equals(HeartBeat.id)) {
                continue;
            }
            long timeDifference = currentTime - member.lastModified;
            if (MembershipList.recentFailures.contains(member.id) && timeDifference > ATA_FAIL_BROADCAST_PERIOD) {
                continue;
            }
            Messenger.send(heartbeat, member.ip);
        }
    }

    // Function that implements gossip style heartbeating
    // Calling node will send heartbeat to a random process in the membership
    // service
    private static void doGossip() {
        List<MembershipList.Member> members = new ArrayList<>(MembershipList.getMembers());
        Collections.shuffle(members);
        Iterator<MembershipList.Member> iter = members.iterator();

        String s = getHeartbeatFull();

        int num_sent = 0;
        while (num_sent < NUM_GOSSIPS && iter.hasNext()) {
            MembershipList.Member m = iter.next();
            if (m.id == HeartBeat.id) {
                continue;
            }

            Messenger.send(s, m.ip);
            num_sent++;
        }
    }

    // Function that periodically sends out heartbeat messages based on the
    // heartbeat period.
    public static void main_heartbeats() {
        int period = ATA_HEARTBEAT_PERIOD;
        while (true) {
            try {
                Thread.sleep(period);
            } catch (InterruptedException e1) {
            }

            synchronized (mutex) {
                if (!is_joined) {
                    continue;
                }
                self_counter++;
                MembershipList.update(HeartBeat.id, HeartBeat.self_counter);
                if (MembershipList.getSize() == 1) {
                    introduce();
                    continue;
                }

                if (is_gossip) {
                    doGossip();
                } else {
                    doATA();
                }

                period = is_gossip ? GOSSIP_HEARTBEAT_PERIOD : ATA_HEARTBEAT_PERIOD;
            }
        }
    }

    // Helper function that processes the gossip message
    // See getHeartbeatGossip for string format spec
    private static void processGossip(String serialized) {

        int nextIndex = serialized.indexOf(" ");
        int list_size = Integer.parseInt(serialized.substring(0, nextIndex));
        serialized = serialized.substring(nextIndex + 1);
        for (int i = 0; i < list_size; i++) {
            nextIndex = serialized.indexOf(" ");
            String currentID = serialized.substring(0, nextIndex);
            serialized = serialized.substring(nextIndex + 1);

            nextIndex = serialized.indexOf(" ");
            int currentCounter = Integer.parseInt(serialized.substring(0, nextIndex));
            serialized = serialized.substring(nextIndex + 1);

            MembershipList.update(currentID, currentCounter);
        }
    }

    // Helper function that processes the all-to-all style message
    // See getHeartbeatATA for string format spec
    private static void processATA(String serialized, String senderId) {
        int nextIndex = serialized.indexOf(" ");
        int queue_size = Integer.parseInt(serialized.substring(0, nextIndex));
        serialized = serialized.substring(nextIndex + 1);
        String currentId = "";
        for (int i = 0; i < queue_size; i++) {
            nextIndex = serialized.indexOf(" ");
            currentId = serialized.substring(0, nextIndex);
            MembershipList.addMember(MembershipList.getAddressFromID(currentId), currentId);
            serialized = serialized.substring(nextIndex + 1);
        }
        serialized = serialized.trim();
        MembershipList.update(senderId, Integer.parseInt(serialized));
    }

    // Main function to deal with parsing messages and calling the correct functions
    // to process them.
    public static void process(String serialized, InetAddress sender) {
        if (!is_joined) {
            return;
        }
        // See getHeartbeatFull for string format spec
        int nextIndex;

        // First parse the heartbeat variant of the message, G for gossip, A for ATA
        boolean alg_is_gossip;
        if (serialized.charAt(0) == 'A') {
            alg_is_gossip = false;
        } else if (serialized.charAt(0) == 'G') {
            alg_is_gossip = true;
        } else {
            System.out.println("Invalid message format.");
            return;
        }
        serialized = serialized.substring(2);

        // Parse the algorithm counter, which details how many times the hearbeat
        // variant has switched.
        nextIndex = serialized.indexOf(" ");
        int alg_counter = Integer.parseInt(serialized.substring(0, nextIndex));
        serialized = serialized.substring(nextIndex + 1);

        // If processing a message with a higher algorithm counter, use the provided
        // heartbeat variant instead of the current one.
        if (alg_counter > algorithm_counter) {
            algorithm_counter = alg_counter;
            is_gossip = alg_is_gossip;
            HeartBeat.giveGrace();
        } else if (is_gossip != alg_is_gossip) {
            // Relay the message back if a newer node has joined and is not up to date on
            // the algorithm being used
            String s = getHeartbeatFull();
            Messenger.send(s, sender);
            return;
        }

        // Get the sender's ID
        nextIndex = serialized.indexOf(" ");
        String senderId = serialized.substring(0, nextIndex);
        serialized = serialized.substring(nextIndex + 1);

        // Process the leave queue
        nextIndex = serialized.indexOf(" ");
        int numLeaves = Integer.parseInt(serialized.substring(0, nextIndex));
        serialized = serialized.substring(nextIndex + 1);
        for (int i = 0; i < numLeaves; i++) {
            nextIndex = serialized.indexOf(" ");
            MembershipList.memberLeave(serialized.substring(0, nextIndex));
            serialized = serialized.substring(nextIndex + 1);
        }

        // Process the failure set
        nextIndex = serialized.indexOf(" ");
        int numFailures = Integer.parseInt(serialized.substring(0, nextIndex));
        serialized = serialized.substring(nextIndex + 1);
        for (int i = 0; i < numFailures; i++) {
            nextIndex = serialized.indexOf(" ");
            String id = serialized.substring(0, nextIndex);
            MembershipList.memberFail(id);

            if (id.equals(HeartBeat.id)) {
                doLeave();
                doJoin();
                return;
            }
            serialized = serialized.substring(nextIndex + 1);
        }

        // Call the corresponding process function based on the heartbeat variant
        if (is_gossip) {
            processGossip(serialized);
        } else {
            processATA(serialized, senderId);
        }
    }

    // Function that is responsible for failed node and cleaning it up accordingly
    public static void main_failure_and_cleanup() {
        int alg_period = ATA_HEARTBEAT_PERIOD;
        while (true) {
            try {
                Thread.sleep(alg_period / 5);
            } catch (InterruptedException e) {
                // NOP
            }

            synchronized (HeartBeat.mutex) {
                int t_cleanup = HeartBeat.is_gossip ? GOSSIP_CLEANUP_PERIOD : ATA_CLEANUP_PERIOD;
                int t_fail = HeartBeat.is_gossip ? GOSSIP_FAIL_PERIOD : ATA_FAIL_PERIOD;

                long now = System.currentTimeMillis();
                ArrayList<MembershipList.Member> tmp = new ArrayList<>(MembershipList.getMembers());
                for (MembershipList.Member member : tmp) {
                    long timeDifference = now - member.lastModified;
                    if (timeDifference > t_fail && !MembershipList.recentFailures.contains(member.id)) {
                        Log.log("Local timeout detected for " + member.id + " (" + MembershipList.getVMFromIp(member.ip)
                                + ")");
                        MembershipList.memberFail(member.id);
                    } else if (timeDifference > t_cleanup && MembershipList.recentFailures.contains(member.id)) {
                        MembershipList.cleanupFail(member.id);
                    }
                }

                alg_period = is_gossip ? GOSSIP_HEARTBEAT_PERIOD : ATA_HEARTBEAT_PERIOD;
            }
        }
    }
}
