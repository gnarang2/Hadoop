import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public class Client {

    public static long FILE_WAIT_TIME = 10000;
    public static long MASTER_WAIT_TIME = 1000;
    public static final String ClientPath = "Client/";
    public static FileSystem ClientFileSystem = new FileSystem(ClientPath);
    private static volatile String relevantCount = "1";
    public static volatile String localFileName = new String();

    public static byte[] getFileInfo(String fileName) {
        String message = "";
        if (ClientFileSystem.fileExists(fileName)) {
            message = (Integer.toString((int) ClientFileSystem.getFileSize(fileName)) + '|' + relevantCount);
        }
        return message.getBytes();
    }

    public static void sendDataNodeMessage(InputStream input, OutputStream output, String[] message)
            throws IOException {

        byte[] contents = null;
        byte[] temp = new byte[1024];
        String ack;
        String fileName = message[1];

        output.write(String.join("|", message).getBytes());

        switch (message[0]) {
            case Commands.CD_GET_FILE: {
                int n = input.read(temp);
                if(n < 0){
                    System.out.println( "Unexpected error, try again\n");
                    break;
                }
                ack = new String(temp).substring(0, n);
                if (ack.equalsIgnoreCase(Commands.FILE_BUSY) || ack.equalsIgnoreCase(Commands.FILE_NOT_PRESENT)) {
                    System.out.println(ack + " (try again later) \n");
                    break;
                } else {
                    output.write(Commands.OK.getBytes());
                }
                Integer size = Integer.parseInt(ack.split("\\|")[0]);
                Integer bytesRead = 0;
                byte[] fileShard = new byte[Messenger.MAX_MESSAGE_SIZE];
                ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();

                Integer off = 0;
                Integer currentSize = 0;
                Integer temp1 = 0;
                boolean transfer = true;
                while(bytesRead < size){
                    // Write to file.
                    temp1 = input.readNBytes(fileShard, 0, Math.min(Messenger.MAX_MESSAGE_SIZE, size-bytesRead));
                    bytesStream.write(fileShard);
                    // If file exception comes, call completeDelete();
                    bytesRead += temp1;
                    currentSize += temp1;
                    if(currentSize >= 5000000){
                        contents = bytesStream.toByteArray();
                        contents = Arrays.copyOf(contents, currentSize);
                        if(!Client.ClientFileSystem.storeDataOffset(contents, localFileName, off)){
                            // break.
                            transfer = false;
                        }
                        currentSize = 0;
                        off = 1;
                        bytesStream.reset();
                    }
                }
                if(currentSize != 0){
                    contents = bytesStream.toByteArray();
                    contents = Arrays.copyOf(contents, currentSize);
                    if(!Client.ClientFileSystem.storeDataOffset(contents, localFileName, off)){
                        // break.
                        transfer = false;
                    }
                }

                // while(bytesRead < size){
                //     bytesRead += input.readNBytes(fileShard, 0, Math.min(Messenger.MAX_MESSAGE_SIZE, size-bytesRead));
                //     bytesStream.write(fileShard);
                // }
                // contents = bytesStream.toByteArray();
                // contents = Arrays.copyOf(contents, bytesRead);
                // if (ClientFileSystem.storeData(contents, fileName)) {
                //     System.out.println("File " + fileName + " has been fetched. Check folder. \n");
                // } else {
                //     System.out.println("File " + fileName + " might have been fetched. Check folder. \n");
                // }
                
                if (transfer) {
                    System.out.println("DFS File " + fileName + " with local name " + localFileName +  " has been fetched. Check folder. \n");
                } else {
                    System.out.println("DFS File " + fileName + " with local name " + localFileName + " might have been fetched. Check folder. \n");
                }
                break;
            }
            case Commands.CD_PUT_FILE: {
                // Check in main function whether file exists or not.
                int n = input.read(temp);
                ack = new String(temp).substring(0, n);
                if (!ack.equalsIgnoreCase(Commands.OK)) {
                    System.out.println(ack + " is the problem. Change operation accordingly.\n");
                    // FIX HERE, DataNode crashed.
                    return;
                }
                output.write(getFileInfo(localFileName));
                if (getFileInfo(localFileName).length == 0) {
                    break;
                }
                input.read(temp);
                byte[] fileData = new byte[Messenger.MAX_MESSAGE_SIZE];
                long fileSize = ClientFileSystem.getFileSize(localFileName); // return fileSize
                long offset = 0;
                while (offset < (int)fileSize) {
                    fileData = ClientFileSystem.getFileData(localFileName, (int) offset);
                    output.write(fileData, 0, Math.min(Messenger.MAX_MESSAGE_SIZE, (int) (fileSize - offset)));
                    offset += (long) Math.min(Messenger.MAX_MESSAGE_SIZE, (int) (fileSize - offset));
                    // System.out.println("Number of bytes read: " + Float.toString( (float)offset/(float)fileSize));
                }
                n = input.read(temp);
                ack = new String(temp).substring(0, n);
                if (!ack.equalsIgnoreCase(Commands.OK)) {
                    System.out.println("Unexpected error occurred, file might or might not have been put\n");
                    break;
                }
                break;
            }
            case Commands.CD_DELETE_FILE: {
                int n = input.read(temp);
                ack = new String(temp).substring(0, n);
                break;
            }
            // case Commands.CD_WRITE_FILE: {
            //     int n = input.read(temp);
            //     ack = new String(temp).substring(0, n);
            //     if (ack != Commands.FILE_NOT_PRESENT) {
            //         System.out.println(Commands.FILE_NOT_PRESENT + " (try again later) \n");
            //         break;
            //     } else {
            //         output.write(Commands.OK.getBytes());
            //     }
            //     Integer size = Integer.parseInt(ack.split("|")[0]);
            //     Integer bytesRead = 0;
            //     byte[] fileShard = new byte[Messenger.MAX_MESSAGE_SIZE];
            //     ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
            //     while (bytesRead <= size) {
            //         input.readNBytes(fileShard, bytesRead, Messenger.MAX_MESSAGE_SIZE);
            //         bytesStream.write(fileShard);
            //         bytesRead += Messenger.MAX_MESSAGE_SIZE;
            //     }
            //     contents = bytesStream.toByteArray();
            //     if (contents.length != size) {
            //         System.out.println("File Corrupted: (try again later) \n");
            //         contents = null;
            //         output.write(Commands.CANCEL.getBytes());
            //         break;
            //     }

            //     ClientFileSystem.storeData(contents, fileName);

            //     System.out.println(
            //             "You can now perform operations on the files. Once done, write 'OK' and enter. To cancel, write 'CANCEL.'\n");
            //     BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            //     String command;
            //     while (true) {
            //         System.out.print("\n\nEnter Operation");
            //         command = br.readLine();
            //         if (command.toUpperCase() == Commands.OK) {
            //             System.out.print("\nChanging file.");
            //             break;
            //         } else if (command.toUpperCase() == Commands.CANCEL) {
            //             System.out.println("\nCancelling Update Operation");
            //             break;
            //         } else {
            //             System.out.println("\nEnter Valid Operation");
            //         }
            //     }
            //     if (command.toUpperCase() == Commands.CANCEL) {
            //         output.write(Commands.CANCEL.getBytes());
            //         break;
            //     } else {
            //         output.write(getFileInfo(fileName));
            //     }
            //     input.read(temp);

            //     byte[] fileData = new byte[Messenger.MAX_MESSAGE_SIZE];
            //     long fileSize = ClientFileSystem.getFileSize(fileName); // return fileSize
            //     long offset = 0;
            //     fileData = ClientFileSystem.getFileData(fileName, (int) offset); // returns fileData of size MAX_MESSAGE
            //                                                                      // from offset
            //     while (fileData != null) {
            //         output.write(fileData, (int) offset,
            //                 Math.min(Messenger.MAX_MESSAGE_SIZE, (int) (fileSize - offset)));
            //         offset += Messenger.MAX_MESSAGE_SIZE;
            //         fileData = ClientFileSystem.getFileData(fileName, (int) offset);
            //     }
            //     n = input.read(temp);
            //     ack = new String(temp).substring(0, n);
            //     if (ack.toUpperCase() == Commands.OPERATION_FAILED) {
            //         System.out.println("Operation Failed. Some networking Error.\n");
            //     } else if (ack.toUpperCase() == Commands.OK) {
            //         System.out.println("Operation Performed Succesfully.\n");
            //     } else {
            //         System.out.println("Operation Failed. Some unknown error.\n");
            //         ;
            //     }
            //     break;
            // }
        }
    }

    public static String[] sendMasterMessage(InputStream input, OutputStream output, String[] message)
            throws IOException {

        byte[] contents = new byte[1024];
        String temp;
        switch (message[0]) {
            case Commands.CM_GET_FILE:
            case Commands.CM_WRITE_FILE:
            case Commands.CM_PUT_FILE:
            case Commands.CM_LS:
            case Commands.CM_DELETE_FILE: {
                contents = (message[0] + "|" + message[1]).getBytes();
                output.write(contents);
                contents = input.readAllBytes();
                temp = new String(contents);
                return temp.split("\\|");
            }
            default: {
                return null;
            }
        }
    }

    public static void clientOperations(String request, String lfsName, String dfsName){

        localFileName = lfsName;
        
        switch(request){
            case Commands.CM_LS:{
                String[] message = new String[2];
                message[0] = Commands.CM_LS;
                message[1] = dfsName;
                String[] action = Messenger.ClientTCPSender(Master.masterIPAddress, message);
                for(String data: action){
                    String[] temp = data.split(":");
                    if(temp.length == 3){
                        System.out.println("Machine " + temp[0] + " has file " + dfsName + " with update count " + temp[1] + " and is currently in state: " + temp[2]);
                    }
                }
                break;
            }
            case Commands.PUT:{
                String[] message = new String[2];
                message[0] = Commands.CM_PUT_FILE;
                message[1] = dfsName;
                String[] action = Messenger.ClientTCPSender(Master.masterIPAddress, message);
                if(action[0].equalsIgnoreCase(Commands.TRY_AGAIN_LATER)){
                    System.out.println("File not put, try again later.");
                    break;
                }
                Client.relevantCount = action[action.length - 1];
                message[0] = Commands.CD_PUT_FILE;
                message[1] = dfsName;
                Integer count = 0;
                InetAddress ip;
                for (String i : action) {
                    try {
                        ip = InetAddress.getByName(i);
                        Messenger.ClientTCPSender(ip, message);
                        count += 1;
                        if(count == Commands.NUM_PUTS){
                            break;
                        }
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                        System.out.println("\n Enter a command > ");
                    }
                }
                break;
            }
            case Commands.DELETE:{
                String[] message = new String[2];
                message[0] = Commands.CM_DELETE_FILE;
                message[1] = dfsName;
                String[] action = Messenger.ClientTCPSender(Master.masterIPAddress, message);
                if(action[0].equalsIgnoreCase(Commands.FILE_NOT_PRESENT)){
                    System.out.println("It wasn't deleted because file was not present.");
                    break;
                }
                message[0] = Commands.CD_DELETE_FILE;
                message[1] = dfsName;
                InetAddress ip;
                for (String i : action) {
                    try {
                        ip = InetAddress.getByName(i);
                        Messenger.ClientTCPSender(ip, message);
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                        System.out.println("\n Enter a command > ");
                    }
                }
                break;
            }
            case Commands.GET:{
                String[] message = new String[2];
                message[0] = Commands.CM_GET_FILE;
                message[1] = dfsName;
                String[] action = Messenger.ClientTCPSender(Master.masterIPAddress, message);
                if(action.length > 0 && action[0].equalsIgnoreCase(Commands.FILE_NOT_PRESENT)){
                    System.out.println("File cannot be fetched because file was not present.");
                    break;
                }
                message[0] = Commands.CD_GET_FILE;
                message[1] = dfsName;
                InetAddress ip;
                for (String i : action) {
                    try {
                        ip = InetAddress.getByName(i);
                        Messenger.ClientTCPSender(ip, message);
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                        System.out.println("\n Enter a command > ");
                    }
                }
                break;
            }
        }
    }

}
