import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;


/* 
 * The Messenger class encompasses all operations relating to sending and receiving packets from other machines. 
 * One of the main threads from Runner.java is used to listen for packets, and another thread is used for 
 * processing messages one-by-one.
 */
public class Messenger {
    public static final int MAX_MESSAGE_SIZE = 60000;
    protected static final Charset ENCODING_CHARSET = StandardCharsets.UTF_8;
    private static final Random rand = new Random();

    // The following variables are used for performing report tests.
    public static long outgoingBytesSum = 0;
    public static long bandwidth_startTime = 0;
    public static int packetLossRate = 0;

    public static int UDP_PORT_LISTENER = 8000;
    public static int UDP_PORT_SENDER = 8001;
    public static int TCP_PORT_LISTENER_DATANODE = 8002;
    public static int TCP_PORT_LISTENER_CLIENT = 8003;

    // Class detailing an individual message to be processed. Contains the content
    // of the packet and the InetAddress of the sender machine.
    protected static class Message {
        public Message(byte[] buf, InetAddress address) {
            this.content = buf;
            this.sender = address;
        }

        byte[] content;
        InetAddress sender;
    }

    // Queue used to process messages one-by-one; The listener thread adds messages
    // to the queue, and the processor thread removes messages from the queue.
    protected static ConcurrentLinkedQueue<Message> unprocessed_messages = new ConcurrentLinkedQueue<>();

    // Sends a given String message to the provided InetAddress via UDP.
    static void send(String message, InetAddress ip) {
        // The following if block is used when trying to simulate packet loss rates.
        if (packetLossRate > 0) {
            int randomNum = rand.nextInt(100) + 1;
            if (randomNum <= packetLossRate) {
                Log.log("Dropping packet destined for " + MembershipList.getVMFromIp(ip) + " (" + ip + ")");
                return; // Simulate packet drop, don't send message
            }
        }
        // Log.log("Sending packet to " + MembershipList.getVMFromIp(ip));

        // Encoding set for the message is UTF-8 by default.
        byte[] bytes = message.getBytes(ENCODING_CHARSET);
        if (bytes.length > MAX_MESSAGE_SIZE) {
            throw new RuntimeException("Serialized hearbeat is too big");
        }

        final int MAX_TRIES = 3;
        for (int trial = 0; trial < MAX_TRIES; trial++) {
            try {
                DatagramSocket clientSocket = new DatagramSocket(UDP_PORT_SENDER);
                DatagramPacket packet = new DatagramPacket(bytes, bytes.length, ip, UDP_PORT_LISTENER);
                clientSocket.send(packet);
                outgoingBytesSum += bytes.length;
                clientSocket.close();
                break;
            } catch (IOException e) {
                if (trial == MAX_TRIES) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    // Method run by the listener thread from the Runner class. Constantly
    // listens on the given port for any incoming messages. When a message is
    // received, it is added to the message processing queue.
    public static void main_listener() {
        DatagramSocket serverSocket;
        try {
            serverSocket = new DatagramSocket(UDP_PORT_LISTENER);
        } catch (SocketException e2) {
            throw new RuntimeException(e2);
        }

        while (true) {
            byte[] buf = new byte[MAX_MESSAGE_SIZE];
            DatagramPacket clientPacket = new DatagramPacket(buf, buf.length);
            try {
                serverSocket.receive(clientPacket);
            } catch (IOException e) {
                System.out.println("Caught IO exception while listening for packets");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    System.out.println("Sleep was interrupted while waiting to listen for packets");
                }
                continue;
            }

            unprocessed_messages.add(new Message(buf, clientPacket.getAddress()));
        }
    }

    // Method run by the processor thread from the Runner class. Constantly
    // processes messages in the message queue (which is added to by main_listener)
    // Actual processing of the messages is completed in the HeartBeat class.
    public static void main_processor() {
        while (true) {
            Message message = unprocessed_messages.poll();
            if (message == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    System.out.println("Sleep was interrupted while waiting for messages to process");
                }
                continue;
            }

            synchronized (HeartBeat.mutex) {
                String s = new String(message.content, ENCODING_CHARSET);
                HeartBeat.process(s, message.sender);
            }
        }
    }


        // Method run by the listener thread from the Runner class. Constantly
    // listens on the given port for any incoming messages. When a message is
    // received, it is added to the message processing queue.
    public static void DataNodeTCPListener() {
        ServerSocket fileSocket = null;
        Socket clientSocket = null;
        
        try {
            fileSocket = new ServerSocket(TCP_PORT_LISTENER_DATANODE);
            // System.out.println("Received message from request: ");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
            return;
        }

        while (true) {
            try {
                clientSocket = fileSocket.accept();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("\n Enter a command > ");
                return;
            }
            TCPThreadDataNode ft = new TCPThreadDataNode(clientSocket);
            ft.start();
            try {
                Thread.sleep(50);
            } catch (InterruptedException e1) {
                System.out.println("Sleep was interrupted while waiting to listen for packets");
            }
        }
    }

    // Only entertains client requests
    public static void ClientTCPListener() {
        ServerSocket fileSocket = null;
        Socket clientSocket = null;
        
        try {
            fileSocket = new ServerSocket(TCP_PORT_LISTENER_CLIENT);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
            return;
        }

        while (true) {
            try {
                clientSocket = fileSocket.accept();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("\n Enter a command > ");
                return;
            }
            TCPThreadClient ft = new TCPThreadClient(clientSocket);
            ft.start();
        }
    }


    public static String[] DataNodeTCPSender(InetAddress ip, String[] message){
        // SEND TO TCP_PORT_LISTENER_DATANODE
        Socket clientSocket=null;
        InputStream input = null;
        OutputStream output = null;

        try{
            clientSocket = new Socket(ip, TCP_PORT_LISTENER_DATANODE);
            input = clientSocket.getInputStream();
            output = clientSocket.getOutputStream();
        } catch (IOException e){
            try {
                closeOperation(clientSocket, input, output);
            } catch (IOException e1) {
                e1.printStackTrace();
                System.out.println("\n Enter a command > ");
            }
            return new String[0];
        }
        switch(message[0]){
            case Commands.MP_GET_FILE:{
                String[] reply = new String[0];
                try {
                    reply = DataNode.sendDataNodeMappleMessage(clientSocket, input, output, message);
                } catch (IOException e) {
                    return new String[0];
                }
                return reply;
            }
            case Commands.MD_GET_FILE:{
                Master.sendDataNodeMessage(clientSocket, input, output, message);
                break;
            }
            case Commands.MD_DELETE_CONTENT:{
                Master.sendDataNodeMessage(clientSocket, input, output, message);
                break;
            }
            case Commands.MD_MAPPLE_PROGRESS_CHECK:
            case Commands.MD_CONSOLIDATE:
            case Commands.MD_JUICE_PROGRESS_CHECK:
            case Commands.MD_SCHEDULE_JUICE_TASK:
            case Commands.MD_SCHEDULE_MAPLE_TASK:{
                Scheduler.sendDataNodeMessage(clientSocket, input, output, message);
            }
            case Commands.DD_GET_FILE:{
                DataNode.sendDataNodeMessage(clientSocket, input, output, message); 
                break;
            }
            case Commands.DM_ACK_REPLICATE_FILE:
            case Commands.DM_ACK_WRITE_FILE:
            case Commands.DM_UNKNOWN_ERROR:{
                DataNode.sendMasterMessage(output, message);
                break;
            }
        }


        try{
            closeOperation(clientSocket, input, output);
        } catch (IOException e){
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
        } 

        return new String[0];
    }

    public static String[] ClientTCPSender(InetAddress ip, String[] message){
        // SEND TO TCP_PORT_LISTENER_DATANODE
        Socket clientSocket=null;
        InputStream input = null;
        OutputStream output = null;
        String[] response = null;

        try{
            clientSocket = new Socket(ip, TCP_PORT_LISTENER_CLIENT);
            input = clientSocket.getInputStream();
            output = clientSocket.getOutputStream();
        } catch (IOException e){
            try {
                closeOperation(clientSocket, input, output);
            } catch (IOException e1) {
                e1.printStackTrace();
                System.out.println("\n Enter a command > ");
            }
            return new String[0];
        }

        switch(message[0]){
            case Commands.CM_START_MAPPLE:
            case Commands.CM_MAPPLE_PROGRESS:
            case Commands.CM_GET_FILE:
            case Commands.CM_JUICE_PROGRESS:
            case Commands.CM_START_JUICE:
            case Commands.CM_WRITE_FILE:
            case Commands.CM_PUT_FILE:
            case Commands.CM_LS:
            case Commands.CM_DELETE_FILE:{
                try {
                    response = Client.sendMasterMessage(input, output, message);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("\n Enter a command > ");
                }
                break;
            }
            case Commands.PM_GET_FILE:
            case Commands.CD_GET_FILE:
            case Commands.CD_PUT_FILE:
            case Commands.CD_DELETE_FILE:
            case Commands.CD_WRITE_FILE:{
                try {
                    Client.sendDataNodeMessage(input, output, message);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("\n Enter a command > ");
                }
                break;
            }
        }


        try{
            closeOperation(clientSocket, input, output);
        } catch (IOException e){
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
        }
        return response;
    }


    public static void closeOperation(Socket s, InputStream is, OutputStream os) throws IOException{
        if (is!=null){
            is.close(); 
        }

        if(os!=null){
            os.close();
        }

        if (s!=null){
            s.close();
        }
    }

}


// TYPES OF TCP MESSAGES RECEIVED BY DATANODE:
// 1. GET_FILE (sent by Client or DataNode): To output write: Size | UpdateCount | File
// 2. REPLICATE_FILE (sent by master): Lock file for updating replica, put request to get file, initiate GET_FILE request on where file is, unlock when received and updated
// 3. WRITE_FILE (sent by Client): Lock file for writing, return file to client, wait for client to write
// 4. DELETE_FILE (sent by Client): Delete the file from SDFS

// TYPES OF TCP MESSAGES RECEIVED BY MASTER:
// 1. ACK_WRITE_FILE (sent by DataNode)
// 2. ACK_REPLICATE_FILE (sent by DataNode)
// 3. ACK_DELETE_FILE (sent by DataNode) - may or may not need this.
// 4. GET_FILE_MASTER (sent by Client to Master)
// 5. WRITE_FILE (sent by Client)
// 6. DELETE_FILE (sent by Client)
// 7. 

// How replication works? Use UDP.
// Periodically master sends files a datanode is supposed to have. If datanode has lower update number it updates, if it doesn't have file
// then it fetches, if it has file and is not included in list, it deletes the file.



class TCPThreadDataNode extends Thread{
    protected Socket socket;
    protected InputStream input = null;
    protected OutputStream output = null;
    protected byte[] temp = new byte[1024];
    protected String[] message;

    public TCPThreadDataNode(Socket clientSocket){
        this.socket = clientSocket;
    }

    public void run() {
        try {
            input = socket.getInputStream();
            output = socket.getOutputStream();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
            try {
                Messenger.closeOperation(socket, input, output);
            } catch (IOException e1) {
                e1.printStackTrace();
                System.out.println("\n Enter a command > ");
            }
            return;
        }

        // DataNode - Master Communication:
        // DM_ACK_WRITE_FILE: To master, informing file written succesfully, and unlock the file
        // DM_ACK_REPLICATE_FILE: To master, informing file replicated successfully, and unlock file

        // DataNode - DataNode communication
        // DD_GET_FILE: To fetch the files from another DataNode for replication, also inform master file replicated
        
        int n;
        try {
            n = input.read(temp);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
            return;
        }
        String part = new String(temp).substring(0,n);
        message = part.split("\\|");

        
        switch(message[0]){ // parse based on who received the message
            case Commands.DM_ACK_REPLICATE_FILE:
            case Commands.DM_ACK_WRITE_FILE:
            case Commands.DM_UNKNOWN_ERROR:{
                try {
                    Master.processDataNodeMessage(output, message, socket.getInetAddress());
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("\n Enter a command > ");
                }
                break;   
            }
            case Commands.MP_GET_FILE:
            case Commands.MD_CONSOLIDATE:
            case Commands.MD_DELETE_CONTENT:
            case Commands.MD_MAPPLE_PROGRESS_CHECK:
            case Commands.MD_JUICE_PROGRESS_CHECK:
            case Commands.MD_SCHEDULE_MAPLE_TASK:
            case Commands.MD_SCHEDULE_JUICE_TASK:
            case Commands.MD_GET_FILE:
            case Commands.DD_GET_FILE:{ // for replicating
                try {
                    DataNode.processDataNodeMessage(socket, input, output, message);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("\n Enter a command > ");
                }
                break;
            }
            default:{
                break;
            }
        }
        try {
            Messenger.closeOperation(socket, input, output);
        } catch (IOException e1) {
            e1.printStackTrace();
            System.out.println("\n Enter a command > ");
        }      
    }

    
}

// dealing with TCP Requests from the Client
class TCPThreadClient extends Thread {
    protected Socket socket;
    protected InputStream input = null;
    protected OutputStream output = null;
    protected byte[] temp = new byte[1024];
    protected String[] message;

    public TCPThreadClient(Socket clientSocket){
        this.socket = clientSocket;
    }

    public void run() {
        try {
            input = socket.getInputStream();
            output = socket.getOutputStream();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
            try {
                Messenger.closeOperation(socket, input, output);
            } catch (IOException e1) {
                e1.printStackTrace();
                System.out.println("\n Enter a command > ");
            }
            return;
        }

        String[] reply = new String[0];

        // Client - Master Communication:
        // CM_GET_FILE: Received by master which sends file information to client if allowed
        // CM_WRITE_FILE: Received by master which sends file information to client and locks the file until updated if allowed
        // CM_PUT_FILE: Received by master, wants to put a file with a unique name and master returns where to put and updates its datastructures if allowed
        // CM_DELETE_FILE: Received by master, updates datastructures to remove files, sends list of datanodes having file, client makes them delete

        // Client - DataNode Communication:
        // CD_GET_FILE: Received by datanode, which returns file if allowed
        // CD_PUT_FILE: Received by datanode, who wants to put a file, send ack to master after process done
        // CD_WRITE_FILE: Received by datanode, who wants to write a file, send ack to master after process done
        // CD_DELETE_FILE: Received by datanode who wants to delete a file
        int n;
		try { 
			n = input.read(temp);
		} catch (IOException e1) {
            e1.printStackTrace();
            System.out.println("\n Enter a command > ");
            return;
		}
        String part = new String(temp).substring(0,n);

        message = part.split("\\|");

        switch(message[0]){ // parse based on who received the message
            case Commands.CM_MAPPLE_PROGRESS:
            case Commands.CM_JUICE_PROGRESS:
            case Commands.CM_START_JUICE:{
                for(String act: message){
                    System.out.println("Received msg: " + act);
                }
            }
            case Commands.CM_START_MAPPLE:{
                Scheduler.processClientMessage(output, message);
                break;
            }
            case Commands.CM_GET_FILE:
            case Commands.CM_WRITE_FILE:
            case Commands.CM_PUT_FILE:
            case Commands.CM_LS:
            case Commands.CM_DELETE_FILE:{
                try {
					Master.processClientMessage(output, message[0], message[1]);
				} catch (IOException e) {
					e.printStackTrace();
                    System.out.println("\n Enter a command > ");
				}
                break;
            }
            case Commands.PM_GET_FILE:
            case Commands.CD_GET_FILE:
            case Commands.CD_PUT_FILE:
            case Commands.CD_DELETE_FILE:
            case Commands.CD_WRITE_FILE:{
                try {
                    reply = DataNode.processClientMessage(input, output, message);
				} catch (IOException e) {
					e.printStackTrace();
                    System.out.println("\n Enter a command > ");
				}
                break;
            }
            default:{
                break;
            }
        }

        try {
            Messenger.closeOperation(socket, input, output);
            if(reply.length == 5){
                // SEND MESSAGE TO MASTER.
                Messenger.DataNodeTCPSender(Master.masterIPAddress, reply);
            }
        } catch (IOException e1) {
            e1.printStackTrace();
            System.out.println("\n Enter a command > ");
        }      
    }
}