import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

public class JuiceTask extends Task{

    public class NodesTask{

        ArrayList<String> status = new ArrayList<>();
        InetAddress node;
        ArrayList<String> fileName = new ArrayList<>();
        ArrayList<String> keys = new ArrayList<>();

        public NodesTask(InetAddress ip, String[] kStrings, String output){
            this.node = ip;
            for(String k: kStrings){
                this.keys.add(k);
                this.fileName.add(output + "_" + k + ".txt");
                this.status.add(Commands.INCOMPLETE);
            }
        }

        public void markComplete(String key){
            int i = 0;
            for(String k: keys){
                if(k.equalsIgnoreCase(key)){
                    status.set(i,Commands.COMPLETE);
                    break;
                }
                i+=1;

            }
        }

        public void markIncomplete(String key){
            int i = 0;
            for(String k: keys){
                if(k.equalsIgnoreCase(key)){
                    status.set(i,Commands.INCOMPLETE);
                    break;
                }
                i+=1;

            }
        }

        public void markStarted(String key){
            int i = 0;
            for(String k: keys){
                if(k.equalsIgnoreCase(key)){
                    status.set(i,Commands.STARTED);
                    break;
                }
                i+=1;
            }
        }

        public NodesTask clone(InetAddress ip, String out){
            String[] newKeys = new String[keys.size()];
            for(int i = 0; i < keys.size(); i++){
                newKeys[i] = keys.get(i);
            }
            NodesTask newNode = new NodesTask(ip, newKeys, out);
            return newNode;
        }


        public void appendKey(String key, String out){
            status.add(Commands.INCOMPLETE);
            fileName.add(out + "_" + key + ".txt");
            keys.add(key);

        }

        public void changeStatus(String key, String status2) {
            int i = 0;
            for(; i < status.size(); i++){
                if(keys.get(i).equalsIgnoreCase(key)){
                    status.set(i,status2);
                    break;
                }
            }
        }

    }
    
    
    
    public String inputFileName;
    public String outputFileName;
    public String executable;
    public Integer numKeys;
    public String partitionMethod;
    public HashMap<InetAddress, NodesTask> nodesTaskMap = new HashMap<>();
    // private HashMap<String, Long> filesLength = new HashMap<>();
    private ArrayList<ArrayList<String>> keysDivisions = new ArrayList<>();
    public InetAddress mainIp;
    private Random rand = new Random();
    
    public JuiceTask(String input, String output, String exec, String divisions, String partitionMethod){
        this.taskType = Commands.JUICE;
        this.outputFileName = output;
        this.inputFileName = input;
        this.executable = exec;
        // this.partitionMethod = partitionMethod;
        this.numKeys = Integer.parseInt(divisions);

        for(String fileName: Master.SDFS.keySet()){
            if(fileName.length() > this.inputFileName.length()+4){
                if(fileName.substring(0, this.inputFileName.length()).equalsIgnoreCase(this.inputFileName)){
                    String relevantFile = fileName.substring(this.inputFileName.length()+1);
                    String key = relevantFile.substring(0, relevantFile.length()-4);
                    this.keys.add(key);                    
                }
            }
        }
        Integer numMachines = getAliveMachines().size();
        for(String k: this.keys){
            int idx = rand.nextInt(numMachines);
            while(keysDivisions.size() <= idx+1){
                keysDivisions.add(new ArrayList<>());
            }
            keysDivisions.get(idx).add(k);
        }
    }

    
    public boolean rescheduleNodesTask(InetAddress ip) {
        if(!minMachinesCheck()){
            return false;
        }
        if(ip.equals(this.mainIp)){
            this.mainIp = null;
        }
        NodesTask toRemove = nodesTaskMap.get(ip);
        nodesTaskMap.remove(ip);
        while(toRemove.keys.size() > 0){
            String key = toRemove.keys.get(0);
            toRemove.keys.remove(0);
            InetAddress newIP = selectLeastBusyNode();
            nodesTaskMap.get(newIP).appendKey(key, this.inputFileName);
        }
        return false;
    }

    public boolean scheduleTask() {
        //TODO hash partitioning...
        
        if(!minMachinesCheck()){
            return false;
        }
        for(ArrayList<String> temp: keysDivisions){
            InetAddress ip = selectLeastBusyNode();
            String[] tempArray = new String[temp.size()];
            int i = 0;
            for(String k: temp){
                tempArray[i] = k;
                i+=1;
            }
            NodesTask tempTask = new NodesTask(ip, tempArray, this.inputFileName);
            addNodesTaskToIp(tempTask);    
        }

        return true;
    }

    
    private void addNodesTaskToIp(NodesTask task) {
        InetAddress ip = task.node;
        if(nodesTaskMap.containsKey(ip)){
            for(String k: task.keys){
                nodesTaskMap.get(ip).appendKey(k, this.inputFileName);
            }
            return;
        }
        nodesTaskMap.put(ip, task);
    }

    public Integer getNumIpTask(InetAddress ip) {        
        if(nodesTaskMap.containsKey(ip)){
            return nodesTaskMap.get(ip).status.size();    
        } else {
            return 0;
        }
        
    }

    
    public void changeStatus(InetAddress ip, String key, String status) {
        if(nodesTaskMap.containsKey(ip)){
            nodesTaskMap.get(ip).changeStatus(key, status);
        }
    }

    
    public String getInputFileName() {
        return this.inputFileName;
    }

    
    public String getOutputFileName() {
        return this.outputFileName;
    }

    
    public String[] generateSchedulerMessageForIp(InetAddress ip) {
        if(!nodesTaskMap.containsKey(ip)){
            return new String[0];
        }
        Integer numTasks = getNumIpTask(ip);
        String[] message = new String[5+numTasks];
        message[0] = Commands.MD_SCHEDULE_JUICE_TASK;
        message[1] = getFileNames()[0];
        message[2] = getFileNames()[1];
        message[3] = getFileNames()[2];
        message[4] = Commands.KEYS;

        Integer i = 5;
        for(String s: nodesTaskMap.get(ip).fileName){
            message[i] = s;
            i+=1;
        }
        return message;
    }

    
    public String[] getFileNames() {
        String[] fileNames = new String[3];
        fileNames[0] = inputFileName;
        fileNames[1] = outputFileName;
        fileNames[2] = executable;
        return fileNames;
    }

    
    public Set<InetAddress> getIpList() {
        return nodesTaskMap.keySet();
    }

    
    public String[] generateProgressMessageForIp(InetAddress ip) {
        if(!nodesTaskMap.containsKey(ip)){
            return new String[0];
        }
        Integer numTasks = getNumIpTask(ip);
        String[] message = new String[5+numTasks];
        message[0] = Commands.MD_JUICE_PROGRESS_CHECK;
        message[1] = getFileNames()[0];
        message[2] = getFileNames()[1];
        message[3] = getFileNames()[2];
        message[4] = Commands.KEYS;
        Integer i = 5;
        for(String s: nodesTaskMap.get(ip).fileName){
            message[i] = s;
            i+=1;
        }
        if(i == 5){
            return new String[0];
        }
        message = Arrays.copyOfRange(message, 0, i);
        return message;
    }

    
    public boolean areTasksComplete() {
        for(InetAddress ip: nodesTaskMap.keySet()){
            for(String s: nodesTaskMap.get(ip).status){
                if(!s.equalsIgnoreCase(Commands.COMPLETE)){
                    return false;
                }
            }
        }
        return true;
    }

    
    public InetAddress getMainIp() {
        if(mainIp == null){
            ArrayList<InetAddress> ipList = new ArrayList<>();
            for(InetAddress ip: getAliveMachines()){
                if(nodesTaskMap.containsKey(ip)){
                    ipList.add(ip);
                }
            }
            if(ipList.size() > 0){
                mainIp = ipList.get(this.rand.nextInt(ipList.size()));
            }
        }
        return mainIp;
    }


    private String findKey(String key){
        for(InetAddress ip: nodesTaskMap.keySet()){
            for(String s: nodesTaskMap.get(ip).keys){
                if(s.equalsIgnoreCase(key)){
                    return ip.getHostAddress();
                }
            }
        }
        return new String("");
    }

    
    public String[] generateConsolidationMessage() {
        String[] message = new String[5+2*this.keys.size()];
        message[0] = Commands.MD_CONSOLIDATE;
        message[1] = getFileNames()[0];
        message[2] = getFileNames()[1];
        message[3] = getFileNames()[2];
        message[4] = Commands.KEYS;
        Integer i = 5;
        for(String s: this.keys){
            String ip = findKey(s);
            message[i] = ip;
            message[i+1] = s;
            i+=2;
        }
        if(i == 5){
            return new String[0];
        }
        message = Arrays.copyOfRange(message, 0, i);
        return message;
    }

    
    
}
