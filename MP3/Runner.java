//Main class
public class Runner {
    public static long startRuntime = 0;

    // Main class that will start 5 threads
    // message listener - responsible for listening for messages at port
    // message processor - responsible for processing all messages
    // heartbeat - responsible for sending heartbeats
    // standard input monitor - responsible for handling command line inputs
    // main failure and cleanup - responsible for failure and cleanup
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println(
                    "First arg must be filename.");
            return;
        }
        Log.filename = args[0];

        startRuntime = System.currentTimeMillis();
        Log.openFile();
        FileSystem.setUp();

        Thread threads[] = { new Thread(() -> Messenger.main_listener(), "message listener"),
                new Thread(() -> Messenger.main_processor(), "message processor"),
                new Thread(() -> Commands.main_commands(), "standard input monitor"),
                new Thread(() -> HeartBeat.main_heartbeats(), "heartbeat"),
                new Thread(() -> HeartBeat.main_failure_and_cleanup(), "failure and cleanup"), 
                new Thread(() -> Messenger.ClientTCPListener(), "client TCP listener"),
                new Thread(() -> Messenger.DataNodeTCPListener(), "DataNode TCP Listener"),
                new Thread(() -> Master.replicationThread(), "ReplicationThread"),
                new Thread(() -> Scheduler.schedulerThread(), "SchedulerThread"), };

        for (Thread t : threads) {
            if (!HeartBeat.ip.equals(Master.masterIPAddress) && t.getName().equalsIgnoreCase("ReplicationThread")) {
                continue;
            }
            if (!HeartBeat.ip.equals(Master.masterIPAddress) && t.getName().equalsIgnoreCase("SchedulerThread")) {
                continue;
            }
            t.start();
            // System.out.println("Starting thread: " + t.getName());
        }

        while (true) {
            for (Thread thread : threads) {
                if (!thread.isAlive()) {
                    if(!HeartBeat.ip.equals(Master.masterIPAddress) && thread.getName().equalsIgnoreCase("ReplicationThread")){
                        continue;
                    }
                    if (!HeartBeat.ip.equals(Master.masterIPAddress) && thread.getName().equalsIgnoreCase("SchedulerThread")) {
                        continue;
                    }
                    String m = "ERROR: the \"" + thread.getName() + "\" thread has exited ";
                    Log.log(m);
                    System.out.println(m);
                    System.exit(1);
                }
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {// NOP
            }
        }
    }
}
