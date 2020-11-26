import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.io.IOException;

// Class that handles all user inputs or commands by reading
// from standard input and calling the appropriate function.
public class Commands {
    public static final String JOIN_GROUP = "JOIN";
    public static final String LEAVE_GROUP = "LEAVE";
    public static final String PRINT_ID = "ID";
    public static final String MEMBERSHIP_LIST = "MEMBERS";
    public static final String CHANGE_TO_GOSSIP = "GOSSIP";
    public static final String CHANGE_TO_ALL = "ALL";
    public static final String START_BANDWIDTH = "STARTBANDWIDTH";
    public static final String STOP_BANDWIDTH = "STOPBANDWIDTH";
    public static final String PACKET_LOSS_RATE = "PACKETLOSS";
    public static final String PRINT_MASTER_FILE_INFO = "SDFS";
    public static final String PRINT_LOCAL_FILE_INFO = "STORE";

    public static final String CM_GET_FILE = "GET_FILE";
    
    public static final String CM_WRITE_FILE = "CM_WRITE_FILE";
    public static final String CM_PUT_FILE = "CM_PUT_FILE";
    public static final String CM_DELETE_FILE = "CM_DELETE_FILE";
    public static final String CM_LS = "LS";
    public static final String CD_GET_FILE = "CD_GET_FILE";
    public static final String CD_PUT_FILE = "CD_PUT_FILE";
    public static final String CD_WRITE_FILE = "CD_WRITE_FILE";
    public static final String CD_DELETE_FILE = "CD_DELETE_FILE";

    public static final String DD_GET_FILE = "DD_GET_FILE";
    public static final String DM_ACK_REPLICATE_FILE = "DM_ACK_REPLICATE_FILE";
    public static final String DM_ACK_WRITE_FILE = "DM_ACK_WRITE_FILE";
    public static final String DM_UNKNOWN_ERROR = "DM_UNKNOWN_ERROR";
    public static final String PRINT_NODES_FILE_MAPPING = "NODES";

    public static final String FILE_NOT_PRESENT = "FILE_NOT_PRESENT";
    public static final long WAIT_TIME = 2000;

    public static final String OK = "OK";
    public static final String READABLE = "READABLE";
    public static final String WRITING = "WRITING";
    public static final String REPLICATING = "REPLICATING";
    public static final String FILE_BUSY = "FILE_BUSY";
    public static final String OPERATION_FAILED = "OPERATION_FAILED";
    public static final String CANCEL = "CANCEL";
    public static final String FILE_PRESENT = "FILE_PRESENT";
    public static final String TRY_AGAIN_LATER = "TRY_AGAIN_LATER";
    public static final String DELETE = "DELETE";
    public static final String ADD = "ADD";
    public static final String DELETE_ALL = "DELETE_ALL";
    public static final String READABLE_DELETE_ALL = "READABLE_DELETE_ALL";
    public static final String PUT = "PUT";
    public static final String GET = "GET";
	public static final String MD_GET_FILE = "MD_GET_FILE";
    public static final String WRONG_INFO = "WRONG_INFO";
    public static final String INCOMPLETE = "INCOMPLETE";
    public static final String COMPLETE = "COMPLETE";
    
	public static final String MD_TEST_TASK = "TEST_TASK_DATANODE";
	public static final String MD_SCHEDULE_TASK = "SCHEDULE_TASK_DATANODE";
    public static final String MD_PROGRESS_CHECK = "MD_PROGRESS_CHECK";
    public static final String DM_TASK_COMPLETE = "TASK_COMPLETE";
	public static final String MAPPLE = "MAPPLE";
    public static final String JUICE = "JUICE";
    public static final String PROGRESS = "PROGRESS";
	public static final String INVALID = "INVALID";
	public static final String STARTED = "STARTED";
	public static final String MD_CONSOLIDATE = "MD_CONSOLIDATE";
	public static final String MD_CONSOLIDATE_CANCEL = "MD_CONSOLIDATE_CANCEL";
	public static final String CONSOLIDATE = "CONSOLIDATE";
	public static final String TASK_NOT_PRESENT = "TASK_NOT_PRESENT";
    public static final String PM_GET_FILE = "PRE_MAPPLE_GET_FILE";
    public static final String MP_GET_FILE = "POST_MAPPLE_GET_FILE";
	public static final String KEYS = "KEYS";
	public static final String CM_START_MAPPLE = "CM_START_MAPPLE";
	public static final String DONE = "DONE";
	public static final String CM_MAPPLE_PROGRESS = "CM_MAPPLE_PROGRESS";
    public static Integer NUM_PUTS = 4;

    

    // Main function that takes care of all commands such as join, leave, print Id,
    // print membership list, changing to all to all, changing to gossip, and start
    // and stop bandwidth that help with checking background bandwidth usage.
    // Function reads from standard input and calls the appropriate helper function
    // based off of what the inputted command is.
    public static void main_commands() {
        String command = "";
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("Enter a command > ");
            try {
                command = br.readLine();
                String[] split = command.split("\\s+");

                switch (split[0].toUpperCase()) {
                    case JOIN_GROUP:
                        synchronized (HeartBeat.mutex){
                            HeartBeat.join();
                            break;
                        }
                    case LEAVE_GROUP:
                        synchronized (HeartBeat.mutex){
                            HeartBeat.leave();
                            break;
                        }
                    case PRINT_ID:
                        synchronized (HeartBeat.mutex){
                            printID();
                            break;
                        }
                    case MEMBERSHIP_LIST:
                        synchronized (HeartBeat.mutex){
                            printMemberList();
                            break;
                        }
                    case PRINT_MASTER_FILE_INFO:
                        Master.printFileInformation();
                        break;
                    case PRINT_LOCAL_FILE_INFO:
                        DataNode.printFileInformation();
                        break;
                    case PRINT_NODES_FILE_MAPPING:{
                        Master.printNodeInformation();
                        break;
                    }
                    case MAPPLE:{
                        if(split.length != 5){
                            break;
                        }
                        if(!checkFileName(PUT, FileSystem.changeFile(split[1]))){
                            break;
                        }
                        if(!checkFileName(PUT, FileSystem.changeFile(split[3]))){
                            break;
                        }
                        if(!checkFileName(PUT, FileSystem.changeFile(split[4]))){
                            break;
                        }
                        Client.MappleJuiceOperations(split[0], split[1], split[2], split[3], split[4]);
                        break;
                    }
                    case CM_LS:{
                        if(split.length != 2){
                            break;
                        }
                        if(!checkFileName(GET, FileSystem.changeFile(split[1]))){
                            break;
                        }
                        Client.clientOperations(CM_LS, "", FileSystem.changeFile(split[1]));
                        break;
                    }
                    case PUT:{
                        if(split.length != 3){
                            break;
                        }
                        if(!checkFileName(PUT, FileSystem.changeFile(split[1]))){
                            break;
                        }
                        if(!checkFileName(GET, FileSystem.changeFile(split[2]))){
                            break;
                        }
                        Client.clientOperations(PUT, FileSystem.changeFile(split[1]), FileSystem.changeFile(split[2]));
                        break;
                    }
                    case DELETE:{
                        if(split.length != 2){
                            break;
                        }
                        if(!checkFileName(DELETE, FileSystem.changeFile(split[1]))){
                            break;
                        }
                        Client.clientOperations(DELETE, "", FileSystem.changeFile(split[1]));
                        break;
                    }
                    case GET:{
                        if(split.length != 3){
                            break;
                        }
                        if(!checkFileName(GET, FileSystem.changeFile(split[1]))){
                            break;
                        }
                        if(!checkFileName(GET, FileSystem.changeFile(split[2]))){
                            break;
                        }
                        Client.clientOperations(GET, FileSystem.changeFile(split[1]), FileSystem.changeFile(split[2]));
                        break;
                    }
                    default:
                        System.out.println("Not a valid command.");
                }
            } catch (IOException e) {
                System.err.println("Error reading standard input. Quitting.");
                break;
            }
        }
    }

    public static boolean checkFileName(String command, String fileName){
        if(command.equalsIgnoreCase(PUT) && !Client.ClientFileSystem.fileExists(fileName)){
            System.out.println("File not there in the client folder.");
            return false;
        }
        if(fileName.indexOf("\\|") != -1){
            System.out.println("File cannot contain character '\\|'.");
            return false;
        }
        return true;
    }

    // Function that takes care of printing the Id of the current node
    public static void printID() {
        System.out.println(HeartBeat.id + " (" + MembershipList.getVMFromIp(HeartBeat.ip) + ")");
    }

    // Function that takes care of printing the current membership list of the group
    // that the calling node is a part of
    public static void printMemberList() {
        long now = System.currentTimeMillis();
        System.out.println("" + MembershipList.getSize() + " members");

        ArrayList<String> lines = new ArrayList<>();
        for (MembershipList.Member m : MembershipList.getMembers()) {
            String name = MembershipList.getVMFromIp(m.ip);
            name = name == null ? m.ip.getHostName() : name;

            String line = String.format("%-8s %-35s %6d %6dms old %10s %10s %10s", name, m.id, m.counter,
                    now - m.lastModified, MembershipList.recentFailures.contains(m.id) ? "Failed" : "",
                    MembershipList.recentJoin.contains(m.id) ? "New" : "",
                    MembershipList.recentLeaves.contains(m.id) ? "Leaving" : "");
            lines.add(line);
        }
        lines.sort((String s1, String s2) -> s1.compareTo(s2));

        for (String s : lines) {
            System.out.println(s);
        }
    }
}
