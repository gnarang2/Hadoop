import java.io.File;
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
                    ps = new ProcessBuilder("java", "-jar", "DataNode/Executables/" + this.executable, "DataNode/Executables/" + this.inputFileName, "DataNode/Executables/" + this.outputFileName, this.machineNumber, temp.get(0), temp.get(1)).start();
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
                System.out.println(file.getName());
                // String[] name = file.getName().split("_");
                // if(name.length == 4){
                //     String key = name[4].substring(0, name[4].length() - 4);
                //     if(filesSet.contains(key)){
                //         continue;
                //     } else {
                //         response += key + "|";
                //         filesSet.add(key);
                //     }
                // }
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

    }

    // public class DataNodeJuicePreTask extends TaskMethods{

    //     public DataNodeJuicePreTask(String inputFileName, String outputFileName, String executable,
    //             String[] taskString) {
    //     }

    //     public boolean getInputFile() {
    //         return false;
    //     }

    //     public boolean getExecutable() {
    //         return false;
    //     }

    //     public void execute() {
    //     }

    //     public String checkCompletion() {
    //         return null;
    //     }

    //     public void introduce(String[] taskList) {
        
    //     }
    // }

    // public class DataNodeJuicePostTask extends TaskMethods{

    //     public DataNodeJuicePostTask(String inputFileName, String outputFileName,
    //             String[] taskString) {
    //     }

    //     public boolean getInputFile() {
    //         return false;
    //     }

    //     public boolean getExecutable() {
    //         return false;
    //     }

    //     public void execute() {
    //     }

    //     @Override
    //     public String checkCompletion() {
    //         return null;
    //     }

    //     @Override
    //     public void introduce(String[] taskList) {
    //     }
    // }

    // public class DataNodeMapplePostTask extends TaskMethods{

    //     public DataNodeMapplePostTask(String inputFileName, String outputFileName,
    //             String[] taskString) {
    //     }

    //     public boolean getInputFile() {
    //         return false;
    //     }

    //     public boolean getExecutable() {
    //         return true;
    //     }

    //     public void execute() {
    //     }

    //     @Override
    //     public String checkCompletion() {
    //         return null;
    //     }

    //     @Override
    //     public void introduce(String[] taskList) {
    //     }
    // }


    // public void introduce(String[] taskList){
    //     task.introduce(taskList);
    // }

	// public String responseMessage() {
    //     return this.task.checkCompletion();
    // }
    
    public boolean checkTaskType(String taskType){
        if(taskType.equalsIgnoreCase(Commands.MAPPLE)){
            return true;
        } else {
            return false;
        }
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