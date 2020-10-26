import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.net.InetAddress;
import java.net.UnknownHostException;

/* 
 * The MembershipList class represents a list of members for each individual machine. The primary function of the
 * class is to keep an active record of which machines are in the current membership group, along with providing 
 * various methods to operate on the list. In addition to the main MembershipList, there are queues being maintained that 
 * contain a list of machines that have recently joined/left the group. 
 */
public class MembershipList {

    // Member class contains the information for a singular member in the group.
    // Information for a member includes its ID, InetAddress, the time it was last
    // modified, and its heartbeat counter.
    public static class Member {
        public String id;
        public InetAddress ip;

        public long lastModified;
        public int counter;
    }

    // First few entries are used as introducers; also used to create mapIPToVM
    public static final ArrayList<InetAddress> allVMs = getAllVMs();
    public static final HashMap<InetAddress, String> mapIPToVM = makeIPMap(allVMs, "VM#%02d", 1);
    private static final String JOIN_UPDATE = "JOIN";
    private static final String LEAVE_UPDATE = "LEAVE";
    private static final String FAIL_UPDATE = "FAIL";
    private static final int MAX_QUEUE_SIZE = 3;

    // Data structures for member mapping, set of failures, and recent joins/leaves
    // queues.
    private static HashMap<String, Member> members = new HashMap<>();
    public static Set<String> recentFailures = new HashSet<>();
    public static Queue<String> recentLeaves = new LinkedList<>();
    public static Queue<String> recentJoin = new LinkedList<>();

    // Returns an ArrayList of the InetAddresses of each VM, used to send/receive
    // messages to other machines.
    private static ArrayList<InetAddress> getAllVMs() {
        final String format = "fa20-cs425-g20-%02d.cs.illinois.edu";
        ArrayList<InetAddress> vms = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {
            String domain = String.format(format, i);
            try {
                vms.add(InetAddress.getByName(domain));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        return vms;
    }

    // Used to create a mapping between the InetAddresses and IDs of each VM.
    private static HashMap<InetAddress, String> makeIPMap(ArrayList<InetAddress> ips, String format, int id_offset) {
        HashMap<InetAddress, String> map = new HashMap<>();

        for (int i = 0; i < ips.size(); i++) {
            String name = String.format(format, i + id_offset);

            map.put(ips.get(i), name);
        }

        return map;
    }

    // Returns the VM ID given an InetAddress.
    public static String getVMFromIp(InetAddress ip) {
        return mapIPToVM.get(ip);
    }

    // Returns a specific Member object given an ID.
    public static Member getMember(String memberId) {
        return members.get(memberId);
    }

    // Given an ID, returns the corresponding InetAddress by parsing the ID.
    public static InetAddress getAddressFromID(String memberId) {
        int index = memberId.indexOf("/");
        try {
            return InetAddress.getByName(memberId.substring(index + 1));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    // Updates the heartbeat counter of a machine in the member list. Also
    // changes the time last modified of the updated entry (if actually updated).
    public static void update(String memberId, int counter) {
        if (!members.containsKey(memberId)) {
            addMember(getAddressFromID(memberId), memberId);
            if (members.containsKey(memberId)) {
                members.get(memberId).counter = counter;
            }
        } else if (!recentFailures.contains(memberId)) {
            Member member = members.get(memberId);
            // Check that the update increases the heartbeat counter
            if (counter > member.counter) {
                member.counter = counter;
                member.lastModified = System.currentTimeMillis();
            }
        }
    }

    // Performs a "leave" operation on a machine in the group. Adds the leaving
    // machine to the recent leaves queue, and removes the machine from the main
    // members list.
    public static void memberLeave(String memberId) {
        if (members.containsKey(memberId)) {
            if (recentFailures.contains(memberId)) {
                return;
            }
            if (recentJoin.contains(memberId)) {
                recentJoin.remove(memberId);
            }
            members.remove(memberId);
            if(HeartBeat.ip.equals(Master.masterIPAddress)){
                Master.removeNode(memberId);
            }
            recentLeaves.add(memberId);
            Log.writeToFile(LEAVE_UPDATE, memberId);
            if (recentLeaves.size() > MAX_QUEUE_SIZE) {
                recentLeaves.remove();
            }
        }
    }

    // Performs a "fail" operation on a machine in the group. Adds the failing
    // machine to the set of recent failures, to be removed after the cleanup
    // period has passed (which is checked in the HeartBeat class).
    public static void memberFail(String memberId) {
        if (!recentFailures.contains(memberId)) {
            Log.writeToFile(FAIL_UPDATE, memberId);
            if (recentJoin.contains(memberId)) {
                recentJoin.remove(memberId);
            }
            if (recentLeaves.contains(memberId)) {
                recentLeaves.remove(memberId);
            }
            if (!members.containsKey(memberId)) {
                Member m = new Member();
                m.ip = getAddressFromID(memberId);
                m.id = memberId;
                m.counter = 0;
                members.put(memberId, m);
            }
            members.get(memberId).lastModified = System.currentTimeMillis();
            recentFailures.add(memberId);
        }
    }

    // Performs a "join" operation on a machine in the group. Adds the joining
    // machine to the recent joins queue, and adds the machine from the main
    // members list.
    public static void addMember(InetAddress address, String id) {
        if (members.containsKey(id) || recentLeaves.contains(id) || recentFailures.contains(id)) {
            return;
        }
        Member m = new Member();
        m.id = id;
        m.counter = 0;

        // the member we are adding might not have been initialized yet, so we give them
        // a grace period before expecting hearbeats from them
        m.lastModified = System.currentTimeMillis() + 5000;
        m.ip = address;
        recentJoin.add(id);
        Log.writeToFile(JOIN_UPDATE, id);
        if (recentJoin.size() > MAX_QUEUE_SIZE) {
            recentJoin.remove();
        }
        members.put(id, m);
        if(HeartBeat.ip.equals(Master.masterIPAddress)){
            Master.Nodes.put(address, new ArrayList<String>());
        }
    }

    // Return the size of the current members list.
    public static int getSize() {
        return members.size();
    }

    // Returns the values of all the members as a Collection.
    public static Collection<Member> getMembers() {
        return members.values();
    }

    // Clears the entire members list.
    public static void removeAll() {
        members.clear();
    }

    // Cleans up the members list and failure sets when the corresponding timeout
    // has passed (checked in the HeartBeat class).
    public static void cleanupFail(String memberId) {
        if (!members.containsKey(memberId) || !recentFailures.contains(memberId)) {
            throw new RuntimeException("Member id " + memberId + " not in membership list nor failure queue.");
        }

        members.remove(memberId);
        if(HeartBeat.ip.equals(Master.masterIPAddress)){
            Master.removeNode(memberId);
        }
        recentFailures.remove(memberId);
    }
}
