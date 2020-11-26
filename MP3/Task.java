import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public abstract class Task {
    
    public String taskType;

    public abstract boolean isTaskComplete();
    
    protected InetAddress selectLeastBusyNode(){
        ArrayList<InetAddress> machines = getAliveMachines();
        Integer minNumberOfTasks = Integer.MAX_VALUE;
        InetAddress minIp = null;
        for(InetAddress ip: machines){
            if(minNumberOfTasks > getNumIpTask(ip)){
                minNumberOfTasks = getNumIpTask(ip);
                minIp = ip;
            }
            if(minNumberOfTasks == 0){
                break;
            }
        }
        return minIp;
    }
    public abstract boolean rescheduleNodesTask(InetAddress ip);
    public abstract boolean scheduleTask();
    public abstract Integer getNumIpTask(InetAddress ip);
    public abstract void changeStatus(InetAddress ip, Integer uniqueID, String status);
    public abstract String getInputFileName();
    public abstract String getOutputFileName();
    
    
    public abstract String[] generateSchedulerMessageForIp(InetAddress ip);
    public abstract String[] getFileNames();
    public abstract Set<InetAddress> getIpList();
    public abstract String[] generateProgressMessageForIp(InetAddress ip);

    protected ArrayList<InetAddress> getAliveMachines(){
        ArrayList<InetAddress> list = new ArrayList<>();
        for(MembershipList.Member member: MembershipList.getMembers()){
            if(!member.ip.getHostAddress().equalsIgnoreCase(Master.masterIPAddress.getHostAddress())){
                list.add(member.ip);
            }
        }
        return list;
    }

    protected boolean minMachinesCheck(){
        return getAliveMachines().size() > 0 ? true : false;
    }
    
    public abstract boolean areTasksComplete();
	public abstract InetAddress getMainIp();
    public abstract String[] generateConsolidationMessage();
    
    public HashSet<String> keys = new HashSet<>();
    
	

    
}
