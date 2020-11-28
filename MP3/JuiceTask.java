import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class JuiceTask extends Task{

    
    
    
    
    
    public JuiceTask(String input, String output, String exec, String divisions, String delete, String partitionMethod){
        

    }
    
    
    
    
    
    
    public boolean isTaskComplete() {
        
        return false;
    }

    
    public boolean rescheduleNodesTask(InetAddress ip) {
        
        return false;
    }

    
    public boolean scheduleTask() {
        
        return false;
    }

    
    public Integer getNumIpTask(InetAddress ip) {
        
        return null;
    }

    
    public void changeStatus(InetAddress ip, Integer uniqueID, String status) {
        

    }

    
    public String getInputFileName() {
        
        return null;
    }

    
    public String getOutputFileName() {
        
        return null;
    }

    
    public String[] generateSchedulerMessageForIp(InetAddress ip) {
        
        return null;
    }

    
    public String[] getFileNames() {
        
        return null;
    }

    
    public Set<InetAddress> getIpList() {
        
        return null;
    }

    
    public String[] generateProgressMessageForIp(InetAddress ip) {
        
        return null;
    }

    
    public boolean areTasksComplete() {
        
        return false;
    }

    
    public InetAddress getMainIp() {
        
        return null;
    }

    
    public String[] generateConsolidationMessage() {
        
        return null;
    }

    
    
}
