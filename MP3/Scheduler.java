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


    public static void processClientMessage(OutputStream output, String[] message){
        switch(message[0]){
            case Commands.CM_START_MAPPLE:{
                if(currentTask == null){
                    try {
                        output.write(Commands.OK.getBytes());
                    } catch (IOException e) {
                    }
                    queueMappleTask(message[1], message[2], message[3], message[4]);
                }
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
    

    public static void queueJuiceTask() {

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
        } else {
            currentTask = null;
        }
    }

    private static void sendSchedulerMessage(String taskType, InetAddress ip) {
        switch(taskType){
            case Commands.MAPPLE:{
                String[] message = schedulingMessageCreator(ip);
                Messenger.DataNodeTCPSender(ip, message);
                break;
            }
            // case Commands.JUICE:{
            //     String[] message = schedulingMessageCreator(ip);
            //     Messenger.DataNodeTCPSender(ip, message);
            //     break;
            // }
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
                if(message.length == 4){
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
                scheduleNextTask();
                flag = 0;
            } else {
                if(currentTask.isTaskComplete()){
                    scheduleNextTask();
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
                // case Commands.MD_CONSOLIDATE_CANCEL:
                // case Commands.MD_CONSOLIDATE:{
                //     output.write(msg);
                //     input.read(temp); // we don't care what the node has to say
                //     return;
                // }
                case Commands.MD_CONSOLIDATE:{
                    output.write(msg);
                    int n = input.read(temp);
                    if (n < 0) {
                        return;
                    }
                    ack = new String(temp).substring(0, n);
                    String[] completedIds = ack.split("\\|");
                    int i;
                    break;
                }
                case Commands.MD_PROGRESS_CHECK:
                case Commands.MD_SCHEDULE_TASK:{
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
                        currentTask.changeStatus(ip, Integer.parseInt(id), status);
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
