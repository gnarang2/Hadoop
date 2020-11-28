import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class DataNodeTask {

    public TaskMethods task = null;
    public String input;
    
    public abstract class TaskMethods {

        public abstract String checkCompletion();

        public abstract boolean getInputFile();

        public abstract boolean getExecutable();

        public abstract void execute();

        public abstract void introduce(String[] taskList);

        public abstract String checkCompletion(String id);
    }

    public class DataNodeMapplePreTask extends TaskMethods {

        String inputFileName = new String();
        String outputFileName = new String();
        String executable = new String();
        HashMap<Integer, ArrayList<String>> taskDivisions = new HashMap<>();
        String machineNumber = new String();
        
        public DataNodeMapplePreTask(String inputFileName, String outputFileName, String executable, String[] taskString) {
            this.inputFileName = inputFileName;
            this.outputFileName = outputFileName;
            this.executable = executable;
            try {
                this.machineNumber = MembershipList.getVMFromIp(InetAddress.getLocalHost()).substring(3);
            } catch (UnknownHostException e) {
    
            }
            
            for(int i = 0; i < taskString.length; i+=3){
                ArrayList<String> temp = new ArrayList<>();
                temp.add(taskString[i]);
                temp.add(taskString[i+1]);
                temp.add("0");
                taskDivisions.put(Integer.parseInt(taskString[i+2]), temp);
            }
        }

        private boolean getFile(String file){
            boolean filePresent = false;
            
            String[] message = new String[2];
            message[0] = Commands.CM_GET_FILE;
            message[1] = file;
            String[] action = Messenger.ClientTCPSender(Master.masterIPAddress, message);
            if(action.length > 0 && action[0].equalsIgnoreCase(Commands.FILE_NOT_PRESENT)){
                return filePresent;
            }
            message[0] = Commands.PM_GET_FILE;
            message[1] = file;
            InetAddress ip;
            for (String i : action) {
                try {
                    ip = InetAddress.getByName(i);
                    Messenger.ClientTCPSender(ip, message);
                    if(DistributedFileSystem.DataNodeFileSystem.checkExecutableFolder(inputFileName)){
                        filePresent = true;
                        break;
                    }
                } catch (UnknownHostException e) {

                }
            }
            return filePresent; // get file from SDFS and put it in Executables folder
        }

        public boolean getInputFile() {
            return getFile(inputFileName);
        }

        public boolean getExecutable() {
            return getFile(executable); // get file from SDFS and put it in Executables folder
        }

        public void execute() {
            // for(Integer i: taskDivisions.keySet()){
            //     System.out.println(taskDivisions.get(i));
            // }
            
            Process ps;
            Iterator<Integer> ID = taskDivisions.keySet().iterator();
            Integer currId = ID.next();
            ArrayList<String> temp = taskDivisions.get(currId);
            while(true){
                try {
                    ps = new ProcessBuilder("java", "-jar", "DataNode/" + this.executable, "DataNode/Executables/" + this.inputFileName, "DataNode/Executables/" + this.outputFileName + "_" + Integer.toString(currId), this.machineNumber, temp.get(0), temp.get(1)).start();
                    ps.waitFor();
                    temp.set(2, "1");
                } catch (Exception e) {
                    continue;
                }
                if(ID.hasNext()){
                    currId = ID.next();
                    temp = taskDivisions.get(currId);
                } else {
                    break;
                }
            }
        }

        public String checkCompletion() {
            String response = new String();
            for(Integer i: taskDivisions.keySet()){
                ArrayList<String> temp = taskDivisions.get(i);
                response += String.valueOf(i) + "|";
                if(temp.get(2).equalsIgnoreCase("0")){
                    response += Commands.STARTED + "|";
                } else {
                    response += Commands.COMPLETE + "|";
                }
            }
            response += Commands.KEYS + "|";
            File files = new File("DataNode/Executables");
            HashSet<String> filesSet = new HashSet<>();
            for(File file: files.listFiles()){                
                if(file.getName().length() < this.outputFileName.length()){
                    continue;
                }
                if(!file.getName().substring(0, this.outputFileName.length()).equalsIgnoreCase(this.outputFileName)){
                    continue;
                }
                String[] name = file.getName().split("_");
                // output_id_machineNumber_key.txt
                if(name.length == 4){
                    String key = name[3].substring(0, name[3].length() - 4);
                    filesSet.add(key);
                }
                for(String k: filesSet){
                    response += k + "|";
                }
            }
            return response.substring(0, response.length()-1);
        }

        public void introduce(String[] taskList) {

            for(int i = 0; i < taskList.length; i+=3){
                if(taskDivisions.containsKey(Integer.parseInt(taskList[i+2]))) {
                    continue;
                }
                ArrayList<String> temp = new ArrayList<>();
                temp.add(taskList[i]);
                temp.add(taskList[i+1]);
                temp.add("0");
                taskDivisions.put(Integer.parseInt(taskList[i+2]), temp);
            }
        }

        public String checkCompletion(String name) {
            String id = name.split("_")[1];
            Integer numID = Integer.parseInt(id);
            if(!taskDivisions.containsKey(numID)){
                return Commands.FILE_NOT_PRESENT;
            }
            for(ArrayList<String> temp: taskDivisions.values()){
                if(temp.get(2).equalsIgnoreCase("0")){
                    return Commands.FILE_BUSY;
                }
            }
            if(!DistributedFileSystem.DataNodeFileSystem.checkExecutableFolder(name + ".txt")){
                return Commands.FILE_NOT_PRESENT;
            }
            // fetch file information for name....
            File file = new File("DataNode/Executables/" + name + ".txt");
            return Long.toString(file.length());
        }

    }

    public class DataNodeMapplePostTask extends TaskMethods{

        String inputFileName = new String();
        String outputFileName = new String();
        HashMap<Integer, ArrayList<String>> taskDivisions = new HashMap<>();
        String taskComplete = Commands.INCOMPLETE;
        ArrayList<String> keyList = new ArrayList<>();
        
        public DataNodeMapplePostTask(String inputFileName, String outputFileName, String[] taskString) {
            this.inputFileName = inputFileName;
            this.outputFileName = outputFileName;
            
            int i = 0;
            for(; i < taskString.length; i+=2){
                if(taskString[i].equalsIgnoreCase(Commands.KEYS)){
                    break;
                }
                ArrayList<String> temp = new ArrayList<>();
                temp.add(taskString[i]);
                temp.add("0");
                taskDivisions.put(Integer.parseInt(taskString[i+1]), temp);
            }

            for(; i < taskString.length; i+=1){
                keyList.add(taskString[i]);
            }

        }

        public String checkCompletion() {
            return taskComplete;
        }

        private boolean getFile(InetAddress ip, Integer id, String key) {
            String machineNumber = new String();
            try {
                if (ip.equals(InetAddress.getLocalHost())) {
                    return true;
                }
                machineNumber = MembershipList.getVMFromIp(InetAddress.getLocalHost()).substring(3); 
            } catch (UnknownHostException e) {
            }
            boolean filePresent = false;
            String fileName = outputFileName + "_" + Integer.toString(id);
            String[] message = new String[3];
            message[0] = Commands.MP_GET_FILE;
            message[1] = fileName + "_" + machineNumber + "_" + key + ".txt";
            message[2] = Integer.toString(id);
            if(DistributedFileSystem.DataNodeFileSystem.checkExecutableFolder(fileName)){
                filePresent = true;
                return filePresent;
            }
            System.out.println("Fetching file: " + message[1]);
            String[] reply = Messenger.DataNodeTCPSender(ip, message);
            if(reply.length == 0){
                return filePresent;
            }
            System.out.println("Reply is: " + reply[0]);
            if(reply[0].equalsIgnoreCase(Commands.OK)){
                if(DistributedFileSystem.DataNodeFileSystem.checkExecutableFolder(fileName)){
                    filePresent = true;
                }
            } else if (reply[0].equalsIgnoreCase(Commands.FILE_NOT_PRESENT)){
                filePresent = true;
            }
            return filePresent; // get file from SDFS and put it in Executables folder
        }

        public boolean getInputFile() {
            InetAddress ip = null;
            Boolean returnVal = true;
            for(Integer id: taskDivisions.keySet()){
                if(taskDivisions.get(id).get(1).equalsIgnoreCase("1")){
                    continue;
                }
                if(taskDivisions.containsKey(id)){
                    try {
                        ip = InetAddress.getByName(taskDivisions.get(id).get(0));
                    } catch (UnknownHostException e) {
                        continue;
                    }
                    Boolean keyValue = true;
                    for(String key: keyList){
                        if(!getFile(ip, id, key)){
                            returnVal = false;
                            keyValue = false;
                        }
                    }
                    if(keyValue){
                        taskDivisions.get(id).set(1, "1");
                    }
                }
            }
            return returnVal;
        }

        public boolean getExecutable() {
            return true;
        }

        private void addToFile(File toAddTo, File toBeAdded){
            try {
                FileWriter permFile = new FileWriter(toAddTo, true);
                FileReader tempFile = new FileReader(toBeAdded);
                BufferedWriter out = new BufferedWriter(permFile);
                BufferedReader in = new BufferedReader(tempFile);
                String str;
                while ((str = in.readLine()) != null) {
                    out.write(str);
                    out.write("\n");
                }
                in.close();
                out.close();
            } catch (IOException e) {
            }
        }

        private void combineFiles(ArrayList<File> listOfFiles, String name){
            // delete files too.....
            String filePath = "DataNode/Executables/" + this.outputFileName + "_" + name + ".txt"; 
            File CombinedFile = new File(filePath);
            for(File file: listOfFiles){
                addToFile(CombinedFile, file);
                file.delete();
            }
        }

        private void consolidate(){
            // combine all of the files based by key....
            while(keyList.size() > 0){
                String key = keyList.remove(0);
                File files = new File("DataNode/Executables");
                ArrayList<File> consolidateFiles = new ArrayList<>();
                for(File file: files.listFiles()){
                    String fileName = file.getName();
                    if(fileName.length() < outputFileName.length()+key.length()+4){
                        continue;
                    }
                    if(!fileName.substring(0, outputFileName.length()).equalsIgnoreCase(outputFileName)){
                        continue;
                    }
                    if(!fileName.substring(fileName.length()-4-key.length(), fileName.length() - 4).equalsIgnoreCase(key)){
                        continue;
                    }
                    consolidateFiles.add(file);
                }
                combineFiles(consolidateFiles, key);
            }
        }

        private void putInSDFS(){
        }

        public void execute() {
            // consolidate();
            System.out.println("Consolidated....");
            // putInSDFS();
            
            taskComplete = Commands.COMPLETE;
            
        }

        public void introduce(String[] taskList) {
            for(int i = 0; i < taskList.length; i+=2){
                if(taskList[i].equalsIgnoreCase(Commands.KEYS)){
                    break;
                }
                ArrayList<String> temp = taskDivisions.get(Integer.parseInt(taskList[i+1]));
                if(temp.get(1).equalsIgnoreCase("0")){
                    temp.set(0, taskList[i]);
                }
            }
        }

        public String checkCompletion(String id) {
            return null;
        }

    }


    // public void introduce(String[] taskList){
    //     task.introduce(taskList);
    // }

	// public String responseMessage() {
    //     return this.task.checkCompletion();
    // }
    
    public boolean checkTaskType(String taskType){
        if(taskType.substring(0, Commands.MAPPLE.length()).equalsIgnoreCase(Commands.MAPPLE)){
            return true;
        } else {
            return false;
        }
    }

    
    public DataNodeTask(String taskType, String inputFileName, String outputFileName, String[] taskString) {

        this.input = inputFileName;

        if(checkTaskType(taskType)){
            this.task = new DataNodeMapplePostTask(inputFileName, outputFileName, taskString);
            TaskThread ft = new TaskThread(this.task);
            ft.start();
        } 
        // else {
        //     task = new DataNodeJuicePreTask(inputFileName, outputFileName, executable, taskString);
        //     TaskThread ft = new TaskThread(task);
        //     ft.start();
        // }
    }


    public DataNodeTask(String taskType, String inputFileName, String outputFileName, String executable, String[] taskString) {

        this.input = inputFileName;

        if(checkTaskType(taskType)){
            this.task = new DataNodeMapplePreTask(inputFileName, outputFileName, executable, taskString);
            TaskThread ft = new TaskThread(this.task);
            ft.start();
        } 
        // else {
        //     task = new DataNodeJuicePreTask(inputFileName, outputFileName, executable, taskString);
        //     TaskThread ft = new TaskThread(task);
        //     ft.start();
        // }
    }

    // public DataNodeTask(String inputFileName, String outputFileName, String[] taskString) {
    //     if(checkTaskType(executable)){
    //         task = new DataNodeMapplePostTask(inputFileName, outputFileName, taskString);
    //         TaskThread ft = new TaskThread(task);
    //         ft.start();
    //     } else {
    //         task = new DataNodeJuicePostTask(inputFileName, outputFileName, taskString);
    //         TaskThread ft = new TaskThread(task);
    //         ft.start();
    //     }
    // }
    
}


// dealing with TCP Requests from the Client
class TaskThread extends Thread {

    DataNodeTask.TaskMethods task = null;

    public TaskThread(DataNodeTask.TaskMethods task){
        this.task = task;
    }

    public void run() {

        while(!task.getInputFile()){

        }

        while(!task.getExecutable()){

        }

        task.execute();
        
     
    }

}