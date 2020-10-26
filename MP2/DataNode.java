import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;


public class DataNode {
    protected static ConcurrentLinkedQueue<Messenger.Message> unprocessed_messages = new ConcurrentLinkedQueue<>();
    protected static final Charset ENCODING_CHARSET = StandardCharsets.UTF_8;
    // protected static DistributedFileSystem SDFS = new DistributedFileSystem();

    public static String[] constructMasterMessage(String ack, String response, String fileName, String toMark){
        String[] reply = new String[5];
        reply[0] = ack;
        reply[1] = response;
        reply[2] = fileName;
        reply[3] = Integer.toString(DistributedFileSystem.getCount(fileName));
        reply[4] = toMark;
        return reply;
    }
    

    // TO DO: EVERYWHERE WHERE YOU CREATE SUBSTRIING ON n CHECK IF N != -1.
    public static boolean processDataNodeMessage(Socket socket, InputStream input, OutputStream output, String[] message) throws IOException{
        // first Output.write(fileInfo), wait for "send", output.write(file), wait for "ok".
        // Inform master.
        // DD_GET_FILE
        String requestType = message[0];
        String fileName = message[1];
        byte[] temp = new byte[1024];
        byte[] contents = null;
        String ack = new String();
        switch(requestType){
            case Commands.MD_GET_FILE:{
                Integer updateCount = Integer.parseInt(message[3]);
                InetAddress ip = InetAddress.getByName(message[2]);

                

                // CHECK WHETHER YOU NEED FILE???
                if(!DistributedFileSystem.fileFree(fileName)){
                    // tell master already processing request.
                    output.write(Commands.FILE_BUSY.getBytes());
                    Messenger.closeOperation(socket, input, output);
                    break;
                }

                if(updateCount == DistributedFileSystem.getCount(fileName)){
                    // INFORM MASTER REPLICATION ALREADY IN PROCESS.
                    output.write(Commands.REPLICATING.getBytes());
                    Messenger.closeOperation(socket, input, output);
                    break;
                } else if (updateCount > DistributedFileSystem.getCount(fileName)) {
                    DistributedFileSystem.FileInformation newFile = new DistributedFileSystem.FileInformation(fileName, updateCount, true);
                    newFile.status = Commands.REPLICATING;
                    DistributedFileSystem.localFiles.put(fileName, newFile);
                    output.write(Commands.OK.getBytes());
                    Messenger.closeOperation(socket, input, output);
                    String[] newMessage = new String[2];
                    newMessage[0] = Commands.DD_GET_FILE;
                    newMessage[1] = fileName;
                    Messenger.DataNodeTCPSender(ip, newMessage);
                    return false;
                } else {
                    output.write(Commands.WRONG_INFO.getBytes());
                    input.read(temp); // TO FIX?????
                    output.write((DistributedFileSystem.getCount(fileName) + "|" + DistributedFileSystem.getStatus(fileName)).getBytes());
                    // MASTER doesn't have your file information. Send it over for it to adjust.
                    // Send to master: updateCount, status.
                    Messenger.closeOperation(socket, input, output);
                    break;
                }
            }
            case Commands.DD_GET_FILE:{
                // SIMILAR TO CD_GET_FILE
                byte[] fileInfo = DistributedFileSystem.getFileInfo(fileName); // consists of FILE_SIZE | UPDATE_COUNT            
                if(fileInfo.length == 0){
                    output.write(Commands.FILE_NOT_PRESENT.getBytes());
                    break;
                }

                if(!DistributedFileSystem.fileFree(fileName)){
                    output.write(Commands.FILE_BUSY.getBytes());
                    break;
                }

                output.write(fileInfo);
                
                int n = input.read(temp);
                ack = new String(temp).substring(0, n);
                if (!ack.equalsIgnoreCase(Commands.OK)){
                    break;
                }
                byte[] fileData = new byte[Messenger.MAX_MESSAGE_SIZE];
                long fileSize = DistributedFileSystem.getFileSize(fileName); // return fileSize
                long offset = 0;
                while (offset < fileSize) {
                    fileData = DistributedFileSystem.getFileData(fileName, (int) offset);
                    output.write(fileData, 0, Math.min(Messenger.MAX_MESSAGE_SIZE, (int) (fileSize - offset)));
                    offset += (long) Math.min(Messenger.MAX_MESSAGE_SIZE, (int) (fileSize - offset));
                }
                return true;
            }
            default:{
                return true;
            }
        }
        return true;
    }
    // TO DO: Remove throw IO Exception. Deal with exceptions here only.
    public static String[] processClientMessage(InputStream input, OutputStream output, String[] operation) throws IOException{ // remove throws IO Exception.
        
        String[] reply = new String[0];
        String fileName = operation[1];
        String ack; 

        switch(operation[0]){
            case Commands.CD_GET_FILE:{
                if(!DistributedFileSystem.fileFree(fileName)){
                    output.write(Commands.FILE_BUSY.getBytes());
                    reply = constructMasterMessage(Commands.DM_UNKNOWN_ERROR, Commands.FILE_BUSY, fileName, Commands.WRITING);
                    return reply;
                }
                byte[] fileInfo = DistributedFileSystem.getFileInfo(fileName); // consists of FILE_SIZE | UPDATE_COUNT            
                if(fileInfo.length == 0){
                    output.write(Commands.FILE_NOT_PRESENT.getBytes());
                    reply = constructMasterMessage(Commands.DM_UNKNOWN_ERROR, Commands.FILE_NOT_PRESENT, fileName, Commands.DELETE);
                    return reply;
                }
                output.write(fileInfo);
                
                int n = input.read(fileInfo);
                if(n < 0){
                    return new String[0];
                }
                ack = new String(fileInfo).substring(0,n);
                if(ack.equalsIgnoreCase(Commands.OK)){
                    byte[] fileData = new byte[Messenger.MAX_MESSAGE_SIZE];
                    long fileSize = DistributedFileSystem.getFileSize(fileName); // return fileSize
                    long offset = 0;
                    while (offset < fileSize) {
                        fileData = DistributedFileSystem.getFileData(fileName, (int) offset);
                        output.write(fileData, 0, Math.min(Messenger.MAX_MESSAGE_SIZE, (int) (fileSize - offset)));
                        offset += (long) Math.min(Messenger.MAX_MESSAGE_SIZE, (int) (fileSize - offset));
                    }
                }
                return new String[0];
            }
            case Commands.CD_PUT_FILE:{ // Returns non empty string 
                byte[] fileInfo = new byte[1024];
                String fileSize;
                String[] message;
                Integer bytesRead = 0;
                byte[] contents = new byte[0];

                if(!DistributedFileSystem.fileFree(fileName)){
                    output.write(Commands.FILE_BUSY.getBytes());
                    reply = constructMasterMessage(Commands.DM_UNKNOWN_ERROR, Commands.FILE_BUSY, fileName, Commands.WRITING);
                    return reply;
                }

                DistributedFileSystem.markWriteBusy(fileName);

                output.write(Commands.OK.getBytes());
                
                int n = input.read(fileInfo); // FILE_SIZE | UPDATE_COUNT
                if(n == -1){
                    reply = constructMasterMessage(Commands.DM_UNKNOWN_ERROR, Commands.CANCEL, fileName, !DistributedFileSystem.localFiles.containsKey(fileName) ? Commands.DELETE : Commands.READABLE);
                    DistributedFileSystem.unmarkwriteBusy(fileName);
                    return reply;
                }
                
                fileSize = new String(fileInfo).substring(0, n);
                message = fileSize.split("\\|");
                if(message.length != 2){
                    reply = constructMasterMessage(Commands.DM_UNKNOWN_ERROR, Commands.CANCEL, fileName, !DistributedFileSystem.localFiles.containsKey(fileName) ? Commands.DELETE : Commands.READABLE);
                    DistributedFileSystem.unmarkwriteBusy(fileName);
                    return reply;
                }

                fileSize = message[0];
                Integer updateCount = Integer.parseInt(message[1]); // Whenever PUT Request comes from client, UpdateCount = 0, Otherwise dataNode.
                output.write(Commands.OK.getBytes());
                Integer size = Integer.parseInt(new String(fileSize));

                byte[] fileShard = new byte[Messenger.MAX_MESSAGE_SIZE];
                ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
                // Open file.
                Integer off = 0;
                Integer currentSize = 0;
                Integer temp = 0;
                boolean transfer = true;
                while(bytesRead < size){
                    // Write to file.
                    temp = input.readNBytes(fileShard, 0, Math.min(Messenger.MAX_MESSAGE_SIZE, size-bytesRead));
                    bytesStream.write(fileShard);
                    // If file exception comes, call completeDelete();
                    bytesRead += temp;
                    currentSize += temp;
                    if(currentSize >= 5000000){
                        contents = bytesStream.toByteArray();
                        contents = Arrays.copyOf(contents, currentSize);
                        if(!DistributedFileSystem.DataNodeFileSystem.storeDataOffset(contents, fileName, off)){
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
                    if(!DistributedFileSystem.DataNodeFileSystem.storeDataOffset(contents, fileName, off)){
                        // break.
                        transfer = false;
                    }
                }
                // contents = bytesStream.toByteArray();
                // contents = Arrays.copyOf(contents, bytesRead);
                if(!transfer){
                    output.write(Commands.OPERATION_FAILED.getBytes());
                    reply = constructMasterMessage(Commands.DM_UNKNOWN_ERROR, Commands.OPERATION_FAILED, fileName, Commands.DELETE);
                    return reply;
                }
                DistributedFileSystem.putFile(fileName, contents, updateCount);
                // if (!DistributedFileSystem.putFile(fileName, contents, updateCount)) { // change function.
                    // System.out.println("Didn't put file");
                    // output.write(Commands.OPERATION_FAILED.getBytes());
                    // reply = constructMasterMessage(Commands.DM_UNKNOWN_ERROR, Commands.OPERATION_FAILED, fileName, Commands.DELETE);
                    // return reply;
                // } else{
                    // System.out.println("put file");
                output.write(Commands.OK.getBytes());
                reply = constructMasterMessage(Commands.DM_ACK_WRITE_FILE, Commands.OK, fileName, Commands.READABLE);
                DistributedFileSystem.unmarkwriteBusy(fileName);
                return reply;
                // }
            }
            // case Commands.CD_WRITE_FILE:{
            //     String ack;
            //     String fileName = operation[1];
            //     byte[] fileInfo = DistributedFileSystem.getFileInfo(fileName); // consists of FILE_SIZE | UPDATE_COUNT
            //     if(fileInfo == null){
            //         output.write(Commands.FILE_NOT_PRESENT.getBytes());
            //         reply = constructMasterMessage(Commands.DM_UNKNOWN_ERROR, Commands.FILE_NOT_PRESENT, fileName, Commands.DELETE);
            //         return reply;   
            //     }
            //     output.write(fileInfo); 
            //     int n = input.read(fileInfo);
            //     ack = new String(fileInfo).substring(0,n);
            //     if(ack == "OK"){
            //         DistributedFileSystem.markWriteBusy(fileName);
            //         byte[] fileData = new byte[Messenger.MAX_MESSAGE_SIZE];
            //         long fileSize = DistributedFileSystem.getFileSize(fileName); // return fileSize
            //         long offset = 0;
            //         fileData = DistributedFileSystem.getFileData(fileName, (int)offset); // returns fileData of size MAX_MESSAGE from offset
            //         while(fileData != null){
            //             output.write(fileData, (int)offset, Math.min(Messenger.MAX_MESSAGE_SIZE, (int)(fileSize - offset)));
            //             offset += Messenger.MAX_MESSAGE_SIZE;
            //             fileData = DistributedFileSystem.getFileData(fileName, (int)offset);
            //         }
            //     }

            //     String fileSize;
            //     String[] message;
            //     Integer bytesRead = 0;
            //     byte[] contents = new byte[1024];
            //     n = input.read(fileInfo);
            //     fileSize = new String(fileInfo).substring(0,n);
            //     if(fileSize.toUpperCase() == Commands.CANCEL){
            //         DistributedFileSystem.unmarkwriteBusy(fileName);
            //         reply = constructMasterMessage(Commands.DM_UNKNOWN_ERROR, Commands.CANCEL, fileName, Commands.READABLE);
            //         return reply;
            //     }
                                
            //     message = fileSize.split("|");
            //     fileSize = message[0];
            //     output.write(Commands.OK.getBytes());
            //     Integer size = Integer.parseInt(new String(fileSize));
                
            //     byte[] fileShard = new byte[Messenger.MAX_MESSAGE_SIZE];
            //     ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
            //     while(bytesRead <= size){
            //         input.readNBytes(fileShard, bytesRead, Messenger.MAX_MESSAGE_SIZE);
            //         // input timeouts are important here. unmarkWrite is also important here.
            //         bytesStream.write(fileShard);
            //         bytesRead += Messenger.MAX_MESSAGE_SIZE;
            //     }
            //     contents = bytesStream.toByteArray();
            //     Integer updateCount = DistributedFileSystem.getCount(fileName) + 1;
            //     // if(contents.length != size){
            //     //     output.write(Commands.OPERATION_FAILED.getBytes());
            //     //     reply = constructMasterMessage(Commands.DM_UNKNOWN_ERROR, Commands.OPERATION_FAILED, fileName, Commands.DELETE_ALL);
            //     //     return reply;
            //     // } else {
            //     DistributedFileSystem.updateFile(fileName, contents, updateCount);
            //     output.write(Commands.OK.getBytes());
            //     reply = constructMasterMessage(Commands.DM_ACK_WRITE_FILE, Commands.OK, fileName, Commands.READABLE);
            //     return reply;
                
            // }
            case Commands.CD_DELETE_FILE:{ // Returns empty String
                DistributedFileSystem.deleteFile(fileName);
                output.write(Commands.OK.getBytes());
                return new String[0];
            }
        }
        return reply;
    }

	public static void sendDataNodeMessage(Socket clientSocket, InputStream input, OutputStream output, String[] message) {
        
        String ack;
        byte[] temp = new byte[1024];
        byte[] contents = null;
        String fileName = message[1];
        Integer updateCount = 0;
        
        try{
            output.write(String.join("|", message).getBytes());
            int n = input.read(temp);
            if(n < 0){
                DistributedFileSystem.localFiles.remove(message[1]);
                return;
            }
            ack = new String(temp).substring(0, n);
            if (ack.equalsIgnoreCase(Commands.FILE_BUSY) || ack.equalsIgnoreCase(Commands.FILE_NOT_PRESENT)) {
                DistributedFileSystem.localFiles.remove(fileName);
                output.write(Commands.CANCEL.getBytes());
                return;
            } else {
                updateCount = Integer.parseInt(ack.split("\\|")[1]);
                if(updateCount < DistributedFileSystem.getCount(fileName)){
                    output.write(Commands.CANCEL.getBytes());
                    DistributedFileSystem.localFiles.remove(fileName);
                    return;
                } else if(updateCount > DistributedFileSystem.getCount(fileName)){
                    DistributedFileSystem.localFiles.get(fileName).updateCount = updateCount;                
                }
            }

            Integer size = Integer.parseInt(ack.split("\\|")[0]);
            output.write(Commands.OK.getBytes());
            Integer bytesRead = 0;
            byte[] fileShard = new byte[Messenger.MAX_MESSAGE_SIZE];
            ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
            // while(bytesRead < size){
            //     bytesRead += input.readNBytes(fileShard, 0, Math.min(Messenger.MAX_MESSAGE_SIZE, size-bytesRead));
            //     bytesStream.write(fileShard);
            // }
            // contents = bytesStream.toByteArray();
            // contents = Arrays.copyOf(contents, bytesRead);

            Integer off = 0;
            Integer currentSize = 0;
            Integer temp1 = 0;
            boolean transfer = true;
            while(bytesRead < size){
                temp1 = input.readNBytes(fileShard, 0, Math.min(Messenger.MAX_MESSAGE_SIZE, size-bytesRead));
                bytesStream.write(fileShard);
                bytesRead += temp1;
                currentSize += temp1;
                if(currentSize >= 5000000){
                    contents = bytesStream.toByteArray();
                    contents = Arrays.copyOf(contents, currentSize);
                    if(!DistributedFileSystem.DataNodeFileSystem.storeDataOffset(contents, fileName, off)){
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
                if(!DistributedFileSystem.DataNodeFileSystem.storeDataOffset(contents, fileName, off)){
                    transfer = false;
                }
            }
            if (transfer) {
                if(!DistributedFileSystem.localFiles.containsKey(fileName)){
                    DistributedFileSystem.localFiles.put(fileName, new DistributedFileSystem.FileInformation(fileName, updateCount, false));
                }
                DistributedFileSystem.localFiles.get(fileName).status = Commands.READABLE;
                Messenger.closeOperation(clientSocket, input, output);
                String[] reply = constructMasterMessage(Commands.DM_ACK_REPLICATE_FILE, Commands.OK, fileName, Commands.READABLE);
                Messenger.DataNodeTCPSender(Master.masterIPAddress, reply);
            } else {
                DistributedFileSystem.DataNodeFileSystem.completeDelete(fileName);
                DistributedFileSystem.localFiles.remove(fileName);
            }
        } catch (IOException e){
            DistributedFileSystem.DataNodeFileSystem.completeDelete(fileName);
            DistributedFileSystem.localFiles.remove(fileName);
        }
	}

	public static void sendMasterMessage(OutputStream output, String[] message){
        try {
            output.write(String.join("|", message).getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
        }
    }

    public static void printFileInformation(){
        for(String files: DistributedFileSystem.localFiles.keySet()){
            System.out.println(DistributedFileSystem.localFiles.get(files).toString());
        }
    }

}
