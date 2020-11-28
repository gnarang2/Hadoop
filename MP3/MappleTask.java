import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class MappleTask extends Task{

    public class NodesTask{

        public NodesTask(InetAddress ip){
            this.node = ip;
            this.uniqueID = ID;
            ID += 1;
        }

        public NodesTask(InetAddress ip, Integer id){
            this.node = ip;
            this.uniqueID = id;
        }

        String status = Commands.INCOMPLETE;
        InetAddress node;
        Long startOffset;
        Long endOffset;
        Integer uniqueID;

        public void setStartOffset(Long off){
            this.startOffset = off;
        }

        public void setEndOffset(Long off){
            this.endOffset= off;
        }

        public void markComplete(){
            this.status = Commands.COMPLETE;
        }

        public void markIncomplete(){
            this.status = Commands.INCOMPLETE;
        }

        public void markStart(){
            this.status = Commands.STARTED;
        }

        public NodesTask clone(InetAddress ip){
            NodesTask newNode = new NodesTask(ip, this.uniqueID);
            newNode.setStartOffset(this.startOffset);
            newNode.setEndOffset(this.endOffset);
            return newNode;
        }

    }

    public HashMap<InetAddress, ArrayList<NodesTask>> nodesTaskMap = new HashMap<>();
    public String inputFileName;
    public String outputFileName;
    public String executable;
    public Integer numKeys;
    private ArrayList<Long> partitions = new ArrayList<>();
    private String offsetString;
    private Integer ID = 0;
    private InetAddress mainIp = null;
    private ReentrantLock lock = new ReentrantLock();

    public MappleTask(String input, String output, String exec, String offsets){
        this.taskType = Commands.MAPPLE;
        this.outputFileName = output;
        this.inputFileName = input;
        this.executable = exec;
        this.offsetString = offsets;
    }

    public boolean areTasksComplete(){
        if(status.equalsIgnoreCase(Commands.COMPLETE)){
            return true;
        } else{
            for(ArrayList<NodesTask> tempList: nodesTaskMap.values()){
                for(NodesTask temp: tempList){
                    if(!temp.status.equalsIgnoreCase(Commands.COMPLETE)){
                        return false;
                    }
                }
            }
            return true;
        }
    }

    public boolean isTaskComplete(){
        if(status.equalsIgnoreCase(Commands.COMPLETE)){
            return true;
        }
        return false;
    }


    protected void addNodesTaskToIp(NodesTask task){
        InetAddress ip = task.node;
        if(!nodesTaskMap.containsKey(ip)){
            nodesTaskMap.put(ip, new ArrayList<>());
        }
        nodesTaskMap.get(ip).add(task);
    }

    // Pick new member to reschedule the task.
    public boolean rescheduleNodesTask(InetAddress ip){
        if(ip.equals(this.mainIp)){
            this.mainIp = null;
        }
        if(!minMachinesCheck()){
            return false;
        }
        for(NodesTask temp: nodesTaskMap.get(ip)){
            InetAddress newIp = selectLeastBusyNode();
            NodesTask newNode = temp.clone(newIp);
            addNodesTaskToIp(newNode);
        }
        nodesTaskMap.remove(ip);
        return true;
    }

    // Assign offsets to divide file
    public boolean scheduleTask(){
        if(!minMachinesCheck()){
            return false;
        }
        ArrayList<Long> filePartitions = partitionFile(inputFileName);
        for(int i = 0; i < filePartitions.size(); i=i+2){
            InetAddress ip = selectLeastBusyNode();
            NodesTask tempTask = new NodesTask(ip);
            tempTask.setStartOffset(filePartitions.get(i));
            tempTask.setEndOffset(filePartitions.get(i+1));
            addNodesTaskToIp(tempTask);
        }
        return true;
    }

    private ArrayList<Long> partitionFile(String fileName){
        if(partitions.size() > 0){
            return partitions;
        }
        String[] temp = this.offsetString.split(":");
        for(String off: temp){
            partitions.add(Long.valueOf(off));
        }
        return partitions;
    }


    public void changeStatus(InetAddress ip, Integer uniqueID, String status){
        for(NodesTask task: nodesTaskMap.get(ip)){
            if(task.uniqueID == uniqueID){
                task.status = status;
            }
        }
    }

    public Integer getNumIpTask(InetAddress ip) {
        if(nodesTaskMap.containsKey(ip)){
            return nodesTaskMap.get(ip).size();
        }
        return 0;
    }

    public ArrayList<NodesTask> getNodesListByIp(InetAddress ip) {
        return nodesTaskMap.get(ip);
    }

    public String[] getFileNames(){
        String[] fileNames = new String[3];
        fileNames[0] = inputFileName;
        fileNames[1] = outputFileName;
        fileNames[2] = executable;
        return fileNames;
    }

    public String[] generateSchedulerMessageForIp(InetAddress ip){
        Integer numTasks = getNumIpTask(ip);
        ArrayList<NodesTask> list = getNodesListByIp(ip);
        String[] message = new String[4+numTasks*3];
        message[0] = Commands.MD_SCHEDULE_TASK;
        message[1] = getFileNames()[0];
        message[2] = getFileNames()[1];
        message[3] = getFileNames()[2];
        Integer i = 4;
        for(NodesTask task: list){
            message[i] = Long.toString(task.startOffset);
            message[i+1] = Long.toString(task.endOffset);
            message[i+2] = Integer.toString(task.uniqueID);
            i+=3;
        }
        return message;
    }

    public Set<InetAddress> getIpList() {
        return nodesTaskMap.keySet();
    }

    public String[] generateProgressMessageForIp(InetAddress ip) {
        Integer numTasks = getNumIpTask(ip);
        ArrayList<NodesTask> list = getNodesListByIp(ip);
        String[] message = new String[(int)4+numTasks*3];
        message[0] = Commands.MD_PROGRESS_CHECK;
        message[1] = getFileNames()[0];
        message[2] = getFileNames()[1];
        message[3] = getFileNames()[2];
        Integer i = 4;
        for(NodesTask task: list){
            if(task.status.equalsIgnoreCase(Commands.COMPLETE)){
                continue;
            }
            message[i] = Long.toString(task.startOffset);
            message[i+1] = Long.toString(task.endOffset);
            message[i+2] = Integer.toString(task.uniqueID);
            i+=3;
        }
        message = Arrays.copyOfRange(message, 0, i);
        return message;
    }

    public InetAddress getMainIp() {
        if(mainIp == null){
            Random rand = new Random();
            ArrayList<InetAddress> ipList = getAliveMachines();
            if(ipList.size() > 0){
                mainIp = ipList.get(rand.nextInt(ipList.size()));
            }
        }
        return mainIp;
    }

    public String[] generateConsolidationMessage(){
        String[] msg = new String[1024];
        msg[0] = Commands.MD_CONSOLIDATE;
        msg[1] = getFileNames()[0];
        msg[2] = getFileNames()[1];
        msg[3] = getFileNames()[2];
        Integer size = 4;
        
        for(InetAddress ip: nodesTaskMap.keySet()){
            for(NodesTask task: nodesTaskMap.get(ip)){
                msg[size] = ip.getHostAddress();
                msg[size+1] = Integer.toString(task.uniqueID);
                size+=2;
            }
        }
        msg[size] = Commands.KEYS;
        size += 1;
        for(String key: keys){
            msg[size] = key;
            size+=1;
        }


        return Arrays.copyOfRange(msg, 0, size);
    }

    public String getInputFileName() {
        return inputFileName;
    }

    public String getOutputFileName() {
        return outputFileName;
    }
    
}
