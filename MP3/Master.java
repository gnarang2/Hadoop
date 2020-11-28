import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class Master {

    public static class MasterFileInformation {
        public String fileName;

        public MasterFileInformation(InetAddress ip, String name) {
            this.fileName = name;
            addMember(ip, 1);
        }

        public void addMember(InetAddress ip, Integer updateCount) {
            DistributedFileSystem.FileInformation fileInfo = DistributedFileSystem.createFileInformation(this.fileName,
                    updateCount);
            files.put(ip, fileInfo);
        }

        public void addMemberReplica(InetAddress ip, Integer updateCount) {
            DistributedFileSystem.FileInformation fileInfo = DistributedFileSystem
                    .createFileInformationReplica(this.fileName, updateCount);
            files.put(ip, fileInfo);
        }

        public MasterFileInformation(String name) {
            this.fileName = name;
        }

        public MasterFileInformation clone() {
            MasterFileInformation copy;
            copy = new MasterFileInformation(this.fileName);
            for (InetAddress ip : this.files.keySet()) {
                copy.addMember(ip, this.files.get(ip).updateCount);
                copy.files.get(ip).status = this.files.get(ip).status;
            }
            return copy;
        }

        public HashMap<InetAddress, DistributedFileSystem.FileInformation> files = new HashMap<>();
    }

    public static InetAddress masterIPAddress = MembershipList.getAddressFromID("fa20-cs425-g20-01.cs.illinois.edu");

    public static HashMap<String, MasterFileInformation> SDFS = new HashMap<>();
    public static HashMap<InetAddress, ArrayList<String>> Nodes = new HashMap<>();
    private static Integer MAX_REPLICAS = 4;
    private static Random rand = new Random();
    private static ReentrantLock operationsLock = new ReentrantLock();
    private static long REPLICATION_WAIT_TIME = 3000;

    public static void processDataNodeMessage(OutputStream output, String[] message, InetAddress ip)
            throws IOException {

        String response = message[1];
        String fileName = message[2];
        Integer updateCount = Integer.parseInt(message[3]);
        String toMark = message[4];
        switch (message[0]) {
            case Commands.DM_ACK_REPLICATE_FILE:
            case Commands.DM_ACK_WRITE_FILE: {
                updateFileInfo(fileName, updateCount, toMark, ip);
                try {
                    output.write(Commands.OK.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("\n Enter a command > ");
                }
                return;
            }
            case Commands.DM_UNKNOWN_ERROR: { // SIMULATE ALL ERRORS.
                switch (response) {
                    case Commands.FILE_BUSY: {
                        updateFileInfo(fileName, updateCount, toMark, ip);
                        break;
                    }
                    case Commands.OPERATION_FAILED:
                    case Commands.FILE_NOT_PRESENT: {
                        if (SDFS.containsKey(fileName) && SDFS.get(fileName).files.containsKey(ip)) {
                            SDFS.get(fileName).files.remove(ip);
                        }
                        break;
                    }
                    case Commands.CANCEL: {
                        if (toMark.equalsIgnoreCase(Commands.DELETE)) {
                            if (SDFS.containsKey(fileName) && SDFS.get(fileName).files.containsKey(ip)) {
                                SDFS.get(fileName).files.remove(ip);
                            }
                            break;
                        }
                        updateFileInfo(fileName, updateCount, toMark, ip);
                        break;
                    }
                }
            }
        }

    }

    private static byte[] getLatestFileMetaData(String fileName, char operation) {
        byte[] response = new byte[0];
        try {
            if (!operationsLock.tryLock(Commands.WAIT_TIME, TimeUnit.MILLISECONDS)) {
                return response;
            }
            if(!SDFS.containsKey(fileName)){
                if (operationsLock.isHeldByCurrentThread()) {
                    operationsLock.unlock();
                }
                return response;
            }
            MasterFileInformation filesInfo = SDFS.get(fileName);
            Set<InetAddress> ipList = filesInfo.files.keySet();
            Integer maxCount = -1;
            InetAddress maxIP;
            ArrayList<InetAddress> potentialIpList = new ArrayList<>();

            for (InetAddress ip : ipList) {
                DistributedFileSystem.FileInformation file = filesInfo.files.get(ip);
                if (file.status.equalsIgnoreCase(Commands.WRITING)) {
                    if (operationsLock.isHeldByCurrentThread()) {
                        operationsLock.unlock();
                    }
                    return response;
                } else if (file.status != Commands.REPLICATING) {
                    if (file.updateCount > maxCount) {
                        potentialIpList.clear();
                        potentialIpList.add(ip);
                        maxCount = file.updateCount;
                    } else if (file.updateCount == maxCount) {
                        potentialIpList.add(ip);
                    }
                }
            }
            if (potentialIpList.size() > 0) {
                int randomIp = rand.nextInt(potentialIpList.size());
                maxIP = potentialIpList.get(randomIp);
                if (operation == 'W') {
                    markWrite(maxIP, fileName);
                }
                if (operationsLock.isHeldByCurrentThread()) {
                    operationsLock.unlock();
                }
                return maxIP.getHostAddress().getBytes();
            }
            if (operationsLock.isHeldByCurrentThread()) {
                operationsLock.unlock();
            }
            return response;
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
            if (operationsLock.isHeldByCurrentThread()) {
                operationsLock.unlock();
            }
            return response;
        }
    }

    private static byte[] writeFileMetaData(String fileName) {
        String response = "";
        byte[] temp = getLatestFileMetaData(fileName, 'W');
        if (temp.length == 0) {
            return response.getBytes();
        }
        // WILL WORK: However, need to fix in client. TO FIX: ONLY SEND WRITES TO FIRST
        // TWO FILES
        String tempIP = new String(temp);
        InetAddress maxIP;
        Integer maxCount = 1;
        try {
            maxIP = InetAddress.getByName(tempIP);
            markWrite(maxIP, fileName);
            maxCount = SDFS.get(fileName).files.get(maxIP).updateCount + 1;
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
            System.out.println("\n Enter a command > ");
            unmarkWrite(fileName);
            return response.getBytes();
        } catch (NullPointerException e2) {
            e2.printStackTrace();
            System.out.println("\n Enter a command > ");
            unmarkWrite(fileName);
            return response.getBytes();
        }

        try {
            if (!operationsLock.tryLock(Commands.WAIT_TIME, TimeUnit.MILLISECONDS)) {
                unmarkWrite(fileName);
                return response.getBytes();
            }
            Set<InetAddress> ipList = SDFS.get(fileName).files.keySet();
            for (InetAddress ip : ipList) {
                response += ip.getHostAddress() + "|";
                SDFS.get(fileName).files.get(ip).updateCount = maxCount;
            }
            int i = ipList.size();
            InetAddress newNodes;
            ArrayList<InetAddress> nodesList = new ArrayList<>(Nodes.keySet());
            Integer len = Math.min(MAX_REPLICAS, Nodes.keySet().size());
            int k = 0;
            while (i < len && k < 10) {
                newNodes = nodesList.remove(rand.nextInt(nodesList.size()));
                if (ipList.contains(newNodes)) {
                    k++;
                    continue;
                }
                response += newNodes.getHostAddress() + "|";
                SDFS.get(fileName).files.put(newNodes,
                        new DistributedFileSystem.FileInformation(fileName, maxCount, true));
                Nodes.get(newNodes).add(fileName);
                SDFS.get(fileName).files.get(newNodes).updateCount = maxCount;
                // if(i > 2){
                // SDFS.get(fileName).files.get(newNodes).status = Commands.REPLICATING;
                // }
                i++;
                k++;
            }
            response = response + Integer.toString(maxCount);
            if (operationsLock.isHeldByCurrentThread()) {
                operationsLock.unlock();
            }
            markWrite(maxIP, fileName);
            return response.getBytes();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
            unmarkWrite(fileName);
            if (operationsLock.isHeldByCurrentThread()) {
                operationsLock.unlock();
            }
            response = "";
            return response.getBytes();
        }
    }

    // Replication algorithm needs to tackle this: Basically not look at replicas
    // but look at updateCount
    private static void markWrite(InetAddress ip, String fileName) {
        MasterFileInformation filesInfo = SDFS.get(fileName);
        Set<InetAddress> ipList = filesInfo.files.keySet();
        for (InetAddress ipAddress : ipList) {
            filesInfo.files.get(ipAddress).status = Commands.WRITING;
        }
    }

    public static void removeNode(String memberId) {
        try {
            operationsLock.lock();
            InetAddress ip = MembershipList.getAddressFromID(memberId);
            ArrayList<String> temp = Nodes.remove(ip);
            for (String file : temp) {
                if (SDFS.containsKey(file)) {
                    SDFS.get(file).files.remove(ip);
                }
            }
            operationsLock.unlock();
        } catch (NullPointerException e) {
            if (operationsLock.isHeldByCurrentThread()) {
                operationsLock.unlock();
            }
        }

    }

    private static void unmarkWrite(String fileName) {
        MasterFileInformation filesInfo = SDFS.get(fileName);
        Set<InetAddress> ipList = filesInfo.files.keySet();
        for (InetAddress ipAddress : ipList) {
            filesInfo.files.get(ipAddress).status = Commands.READABLE;
        }
    }

    private static byte[] putFileMetaData(String fileName) {
        String message = "";
        try {
            if (!operationsLock.tryLock(Commands.WAIT_TIME, TimeUnit.MILLISECONDS)) {
                return message.getBytes();
            }
            // ERROR IN THIS FUNCTION.
            ArrayList<InetAddress> nodesList = new ArrayList<>(Nodes.keySet());
            InetAddress temp;
            Integer len = Math.min(MAX_REPLICAS, Nodes.keySet().size());
            for (int i = 1; i <= len; i++) {
                temp = nodesList.remove(rand.nextInt(nodesList.size()));
                if (SDFS.containsKey(fileName)) {
                    SDFS.get(fileName).addMember(temp, 1);
                    // if(i > 2){
                    // SDFS.get(fileName).files.get(temp).status = Commands.REPLICATING; // TO DO
                    // CHANGE TO WRITING and ACCORDINGLY
                    // }
                    if (Nodes.containsKey(temp) && Nodes.get(temp) != null) {
                        Nodes.get(temp).add(fileName);
                    } else {
                        ArrayList<String> arr = new ArrayList<String>();
                        arr.add(fileName);
                        Nodes.put(temp, arr);
                    }
                } else {
                    SDFS.put(fileName, new MasterFileInformation(temp, fileName));
                    if (Nodes.containsKey(temp)) {
                        Nodes.get(temp).add(fileName);
                    } else {
                        ArrayList<String> arr = new ArrayList<String>();
                        arr.add(fileName);
                        Nodes.put(temp, arr);
                    }
                }
                message += temp.getHostAddress() + "|";
                if (i == len) {
                    message += "1";
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
            message = "";
        } finally {
            if (operationsLock.isHeldByCurrentThread()) {
                operationsLock.unlock();
            }
        }
        return message.getBytes();
    }

    private static byte[] deleteFileMetaData(String fileName) {
        String message = "";
        try {
            if (!operationsLock.tryLock(Commands.WAIT_TIME, TimeUnit.MILLISECONDS)) {
                return message.getBytes();
            }
            // operationsLock.lock();
            if (!SDFS.containsKey(fileName)) {
                if (operationsLock.isHeldByCurrentThread()) {
                    operationsLock.unlock();
                }
                return message.getBytes();
            }
            ArrayList<InetAddress> nodesList = new ArrayList<>(SDFS.get(fileName).files.keySet());
            SDFS.remove(fileName);
            for (InetAddress ip : nodesList) {
                Nodes.get(ip).remove(fileName);
                message += ip.getHostAddress();
                if (ip != nodesList.get(nodesList.size() - 1)) {
                    message += '|';
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
            message = "";
        } finally {
            if (operationsLock.isHeldByCurrentThread()) {
                operationsLock.unlock();
            }
        }
        return message.getBytes();
    }

    public static void processClientMessage(OutputStream output, String requestType, String fileName)
            throws IOException {

        switch (requestType) {
            case Commands.CM_LS:{
                byte[] fileData = getFileInformation(fileName);
                output.write(fileData);
                return;
            }
            case Commands.CM_GET_FILE: {
                byte[] fileData = getLatestFileMetaData(fileName, 'R');
                if (fileData.length == 0) {
                    output.write(Commands.FILE_NOT_PRESENT.getBytes());
                    return;
                }
                output.write(fileData); // could write null
                return;
            }
            // case Commands.CM_WRITE_FILE:{
            // output.write(getLatestFileMetaData(fileName, 'W')); // could write null
            // return;
            // }
            case Commands.CM_PUT_FILE: {
                byte[] fileMetaData = SDFS.containsKey(fileName) ? writeFileMetaData(fileName)
                        : putFileMetaData(fileName);
                if (fileMetaData.length > 0) {
                    output.write(fileMetaData, 0, fileMetaData.length);
                } else {
                    output.write(Commands.TRY_AGAIN_LATER.getBytes()); // change this to include exactly the problem.
                }
                return;
            }
            case Commands.CM_DELETE_FILE: {
                byte[] fileMetaData = deleteFileMetaData(fileName);
                if (fileMetaData.length < 3) {
                    output.write(Commands.FILE_NOT_PRESENT.getBytes());
                } else {
                    output.write(fileMetaData); // client informs nodes to delete the file.
                }
                return;
            }
        }
    }

    public static void printFileInformation() {
        if (!HeartBeat.ip.equals(Master.masterIPAddress)) {
            System.out.println("Not the master, try different machine.");
            return;
        }
        for (String files : SDFS.keySet()) {
            MasterFileInformation temp = SDFS.get(files);
            for (InetAddress ip : temp.files.keySet()) {
                System.out.println(temp.files.get(ip) + " at machine " + MembershipList.getVMFromIp(ip));
            }
            // System.out.println("\n");
        }
    }

    public static byte[] getFileInformation(String fileName){
        String response = "";
        if(!SDFS.containsKey(fileName)){
            return response.getBytes();
        }
        MasterFileInformation temp = SDFS.get(fileName);
        for (InetAddress ip : temp.files.keySet()) {
            response += MembershipList.getVMFromIp(ip) + ":" + Integer.toString(SDFS.get(fileName).files.get(ip).updateCount) + ":" + SDFS.get(fileName).files.get(ip).status + "|";
        }
        if(response.length() > 1){
            response = response.substring(0, response.length()-1);
        }
        return response.getBytes();
    }

    public static void printNodeInformation() {
        if (!HeartBeat.ip.equals(Master.masterIPAddress)) {
            System.out.println("Not the master, try different machine.");
            return;
        }
        for (InetAddress ip : Nodes.keySet()) {
            for (String files : Nodes.get(ip)) {
                System.out.println("machine " + MembershipList.getVMFromIp(ip) + " has " + files);
            }
        }

    }

    public static void updateFileInfo(String fileName, Integer updateCount, String toMark, InetAddress ip) {
        // Put a lock over replication thread.
        SDFS.get(fileName).files.get(ip).status = toMark;
        SDFS.get(fileName).files.get(ip).updateCount = updateCount;
    }

    public static void sendDataNodeMessage(Socket socket, InputStream input, OutputStream output, String[] message) {

        String requestType = message[0];
        String fileName = message[1];
        byte[] temp = new byte[1024];
        String ack;
        String[] response = null;
        InetAddress ip = socket.getInetAddress();
        try {
            switch (requestType) {
                case Commands.MD_DELETE_CONTENT:{
                    output.write(String.join("|", message).getBytes());
                    int n = input.read(temp);
                    return;
                }
                case Commands.MD_GET_FILE: {
                    Integer updateCount = Integer.parseInt(message[3]);
                    output.write(String.join("|", message).getBytes());
                    int n = input.read(temp);
                    if (n < 0) {
                        return;
                    }
                    ack = new String(temp).substring(0, n);
                    switch (ack) {
                        case Commands.FILE_BUSY: {
                            return;
                        }
                        case Commands.REPLICATING: {
                            return;
                        }
                        case Commands.OK: {
                            // check if SDFS doesn't contain the file.
                            // add to SDFS.
                            operationsLock.lock();
                            if (!SDFS.get(fileName).files.containsKey(ip)) {
                                SDFS.get(fileName).addMemberReplica(ip, updateCount);
                                Nodes.get(ip).add(fileName);
                            }
                            operationsLock.unlock();
                            return;
                        }
                        case Commands.WRONG_INFO: {
                            output.write(Commands.OK.getBytes());
                            n = input.read(temp);
                            if (n < 0) {
                                return;
                            }
                            ack = new String(temp).substring(0, n);
                            response = ack.split("\\|");
                            updateCount = Integer.parseInt(response[0]);
                            String status = response[1];
                            operationsLock.lock();
                            SDFS.get(fileName).files.get(ip).status = status;
                            SDFS.get(fileName).files.get(ip).updateCount = updateCount;
                            operationsLock.unlock();
                        }
                        default: {
                            return;
                        }
                    }
                }
                default: {
                    return;
                }
            }
        } catch (IOException e) {

        }
    }

    public static void replicationThread() {
        InetAddress maxip;
        Integer maxCount;

        while (true) {
            if (operationsLock.isHeldByCurrentThread()) {
                operationsLock.unlock();
            }
            for (String fileName : SDFS.keySet()) {
                MasterFileInformation masterFileInfo = SDFS.get(fileName).clone();
                maxCount = 0;
                maxip = null;
                Integer fileCount = 0;
                for (InetAddress ip : masterFileInfo.files.keySet()) {
                    if (masterFileInfo.files.get(ip).status.equalsIgnoreCase(Commands.WRITING)) {
                        maxCount = 0;
                        break;
                    } else if (masterFileInfo.files.get(ip).updateCount > maxCount) {
                        maxip = ip;
                        maxCount = masterFileInfo.files.get(ip).updateCount;
                    }
                }
                if (maxCount != 0 && maxip != null) {
                    String[] message = new String[4];
                    message[0] = Commands.MD_GET_FILE;
                    message[1] = fileName;
                    message[2] = maxip.getHostAddress();
                    message[3] = Integer.toString(maxCount);

                    for (InetAddress ip : masterFileInfo.files.keySet()) {
                        fileCount += 1;
                        try {
                            if (!operationsLock.tryLock(Commands.WAIT_TIME, TimeUnit.MILLISECONDS)) {
                                continue;
                            }
                        } catch (InterruptedException e) {
                            continue;
                        }
                        if (maxCount > masterFileInfo.files.get(ip).updateCount
                                && SDFS.get(fileName).files.containsKey(ip)
                                && SDFS.get(fileName).files.containsKey(maxip)) {
                            Messenger.DataNodeTCPSender(ip, message);
                            // System.out.println("Sending MD_GET_FILE message to :" + );
                        }
                        if (operationsLock.isHeldByCurrentThread()) {
                            operationsLock.unlock();
                        }
                    }
                    // System.out.println("File Count: " + Integer.toString(fileCount));
                    // System.out.println("OVerall Count: " +
                    // Integer.toString(Math.min(MAX_REPLICAS, Nodes.keySet().size())));
                    if (operationsLock.isHeldByCurrentThread()) {
                        operationsLock.unlock();
                    }
                    if (fileCount != Math.min(MAX_REPLICAS, Nodes.keySet().size())) {
                        // System.out.println("In replicating thread loop.");
                        ArrayList<InetAddress> nodesList = new ArrayList<>(Nodes.keySet());
                        Integer len = Math.min(MAX_REPLICAS, Nodes.keySet().size());
                        Integer k = 0;
                        Integer i = fileCount;
                        InetAddress newNodes;
                        while (i < len && k < 10) {
                            newNodes = nodesList.remove(rand.nextInt(nodesList.size()));
                            if (SDFS.get(fileName).files.containsKey(newNodes)) {
                                k++;
                                continue;
                            }
                            try {
                                if (!operationsLock.tryLock(Commands.WAIT_TIME, TimeUnit.MILLISECONDS)) {
                                    continue;
                                }
                            } catch (InterruptedException e) {
                                continue;
                            }
                            // System.out.println(MembershipList.getVMFromIp(newNodes) + " sending to.");
                            Messenger.DataNodeTCPSender(newNodes, message);
                            if (operationsLock.isHeldByCurrentThread()) {
                                operationsLock.unlock();
                            }
                            i++;
                            k++;
                        }            
                    }
                    if (operationsLock.isHeldByCurrentThread()) {
                        operationsLock.unlock();
                    }
                }

                // Make master ask for files continuously.
            }
            try {
                Thread.sleep(REPLICATION_WAIT_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("\n Enter a command > ");
            }
        }
    }



}
