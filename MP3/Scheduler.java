import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;

public class Scheduler {

    private static ArrayList<Task> taskList = new ArrayList<>();
    private static Task currentTask = null;
    private static long SCHEDULER_WAIT_TIME = 2000;
    private static Integer taskNumber = -1;
    private static Integer flag = 0;


    public static String[] schedulingMessageCreator(InetAddress ip) {
        return currentTask.generateSchedulerMessageForIp(ip);
    }

    public static String[] progressMessageCreator(InetAddress ip) {
        return currentTask.generateProgressMessageForIp(ip);
    }

    public static void queueMappleTask(String fileName, String SDFSInterMediateFileNamePrefix,
            String exec, String offsets) {
        MappleTask task = new MappleTask(fileName, SDFSInterMediateFileNamePrefix, exec, offsets);
        taskList.add(task);
    }

    public static void queueJuiceTask(String sdfs_intermediate_filename_prefix, String sdfs_dest_filename, String juice_exe, String num_juice, String delete, String partitionMethod) {
        JuiceTask task = new JuiceTask(sdfs_intermediate_filename_prefix, sdfs_dest_filename, juice_exe, num_juice, delete, partitionMethod);
        taskList.add(task);
    }


    public static void processClientMessage(OutputStream output, String[] message){
        switch(message[0]){
            case Commands.CM_START_JUICE:{
                try {
                    output.write(Commands.OK.getBytes());
                } catch (IOException e) {
                }
                queueJuiceTask(message[1], message[2], message[3], message[4], message[5], message[6]);
                return;
            }
            case Commands.CM_JUICE_PROGRESS:{
                //TODO
                return;
            }
            case Commands.CM_START_MAPPLE:{
                try {
                    output.write(Commands.OK.getBytes());
                } catch (IOException e) {
                }
                queueMappleTask(message[1], message[2], message[3], message[4]);
                return;
            }
            case Commands.CM_MAPPLE_PROGRESS:{
                try{
                    if(currentTask == null){
                        output.write(Commands.OK.getBytes());
                    } else {
                        for(Task t: taskList){
                            if(t.getInputFileName().equalsIgnoreCase(message[1]) && t.getOutputFileName().equalsIgnoreCase(message[2])){
                                if(t.isTaskComplete()){
                                    output.write(Commands.DONE.getBytes());
                                    currentTask = null;
                                    return;
                                } else {
                                    output.write(Commands.OK.getBytes());
                                    return;
                                }
                            }
                        }
                        output.write(Commands.OK.getBytes());
                    }
                } catch (IOException e){

                }
                return;
            }
        }
    }
    

    public static void scheduleNextTask(){
        if(taskList.size() > taskNumber+1){
            taskNumber += 1;
            currentTask = taskList.get(taskNumber);
            if(!currentTask.scheduleTask()){
                System.out.println("\nNot enough servers to schedule task, waiting to schedule task....");
                return;
            }
            for(InetAddress ip: currentTask.getIpList()){
                sendSchedulerMessage(currentTask.taskType, ip);
            }
        }
    }

    private static void sendSchedulerMessage(String taskType, InetAddress ip) {
        switch(taskType){
            case Commands.MD_DELETE_CONTENT:{
                String[] message = new String[2];
                message[0] = Commands.MD_DELETE_CONTENT;
                message[1] = "null";
                Messenger.DataNodeTCPSender(ip, message);
                break;
            }
            case Commands.MAPPLE:{
                String[] message = schedulingMessageCreator(ip);
                Messenger.DataNodeTCPSender(ip, message);
                break;
            }
            case Commands.JUICE:{
                String[] message = schedulingMessageCreator(ip);
                Messenger.DataNodeTCPSender(ip, message);
                break;
            }
            case Commands.PROGRESS:{
                String[] message = progressMessageCreator(ip);
                if(message.length <= 4){
                    return;
                }
                Messenger.DataNodeTCPSender(ip, message);
                break;
            }
            case Commands.CONSOLIDATE:{
                String[] message = consolidationMessageCreator();
                if(message.length <= 4){
                    break;
                }
                Messenger.DataNodeTCPSender(ip, message);
                break;
            }
        }
    }

    private static String[] consolidationMessageCreator(){
        return currentTask.generateConsolidationMessage();
    }

    public static void schedulerThread() {
        while (true){

            if(currentTask == null){
                for(MembershipList.Member member : MembershipList.getMembers()){
                    sendSchedulerMessage(Commands.MD_DELETE_CONTENT, member.ip);    
                }
                scheduleNextTask();
                flag = 0;
            } else {
                if(currentTask.isTaskComplete()){
                    flag = 0;
                } else{
                    if(!currentTask.areTasksComplete()){
                        for(InetAddress ip: currentTask.getIpList()){
                            sendSchedulerMessage(Commands.PROGRESS, ip);
                        }
                    } else {
                        flag = 1;
                    }
                    if(flag == 1){
                        InetAddress mainIp = currentTask.getMainIp();
                        if(mainIp != null){
                            sendSchedulerMessage(Commands.CONSOLIDATE, mainIp);
                        }
                    }
                }
            }

            try {
                Thread.sleep(SCHEDULER_WAIT_TIME);
            } catch (InterruptedException e) {
                System.out.println("\n Start scheduler again.");
            }
        }
        
    }


    public static void nodeFailed(InetAddress ip) {
        if (currentTask != null) {
            currentTask.rescheduleNodesTask(ip);
        }
    }

	public static void sendDataNodeMessage(Socket socket, InputStream input, OutputStream output, String[] message) {
        String requestType = message[0];
        byte[] msg = String.join("|", message).getBytes();
        byte[] temp = new byte[1024];
        String ack;
        InetAddress ip = socket.getInetAddress();
        try {
            switch (requestType) {
                case Commands.MD_CONSOLIDATE:{
                    output.write(msg);
                    int n = input.read(temp);
                    if (n < 0) {
                        return;
                    }
                    ack = new String(temp).substring(0, n);
                    if(ack.equalsIgnoreCase(Commands.COMPLETE)){
                        currentTask.status = Commands.COMPLETE;
                    }
                    break;
                }
                case Commands.MD_SCHEDULE_JUICE_TASK:{
                    output.write(msg);
                    int n = input.read(temp);
                    if (n < 0) {
                        return;
                    }
                    ack = new String(temp).substring(0, n);
                    String[] completedKeys = ack.split("\\|");
                    for(int i = 1; i < completedKeys.length; i+=2){
                        String key = completedKeys[i];
                        String status = completedKeys[i+1];
                        currentTask.changeStatus(ip, key, status);
                    }
                    return;
                }
                case Commands.MD_JUICE_PROGRESS_CHECK:{
                    output.write(msg);
                    int n = input.read(temp);
                    if (n < 0) {
                        return;
                    }
                    ack = new String(temp).substring(0, n);
                    String[] completedKeys = ack.split("\\|");
                    for(int i = 1; i < completedKeys.length; i+=2){
                        String key = completedKeys[i];
                        String status = completedKeys[i+1];
                        currentTask.changeStatus(ip, key, status);
                    }
                    return;
                }
                case Commands.MD_MAPPLE_PROGRESS_CHECK:
                case Commands.MD_SCHEDULE_MAPLE_TASK:{
                    output.write(msg);
                    int n = input.read(temp);
                    if (n < 0) {
                        return;
                    }
                    ack = new String(temp).substring(0, n);
                    String[] completedIds = ack.split("\\|");
                    int i;
                    for(i = 0; i < completedIds.length; i+=2){
                        String id = completedIds[i];
                        if(id.equalsIgnoreCase(Commands.KEYS)){
                            break;
                        }
                        String status = completedIds[i+1];
                        currentTask.changeStatus(ip, id, status);
                    }
                    for(; i < completedIds.length; i++){
                        String key = completedIds[i];
                        currentTask.keys.add(key);
                    }
                    return;
                }
                default: {
                    return;
                }
            }
        } catch (IOException e) {
        }
	}

}
