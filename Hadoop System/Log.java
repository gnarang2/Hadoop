import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

// Class that is responsible for all logging during the program
public class Log {
    public static String filename = null;
    private static FileWriter writer = null;
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

    // Function that opens the file that all logs will be placed in
    public static void openFile() {
        try {
            writer = new FileWriter(filename, false);
            writer.write("Start Time: " + dateFormat.format(Runner.startRuntime) + "\n");
        } catch (IOException e) {
            throw new RuntimeException("Error opening log file.");
        }
    }

    // Function that closes the file that all logs are placed in
    public static void closeFile() {
        try {
            writer.close();
        } catch (IOException e) {
            System.err.println("Error closing log file.");
        }
    }

    // Function that writes a corresponding message to the file
    public static void writeToFile(String updateType, String updateId) {
        log(updateType + ": " + updateId + " (" + MembershipList.getVMFromIp(MembershipList.getAddressFromID(updateId))
                + ")");
    }

    // Function that logs how many bytes have been sent
    public static void writeBytes(long bytesSent) {
        log("Number of bytes sent: " + Long.toString(bytesSent));
    }

    // Main logging function that logs the VM name and the time associated to it
    public static void log(String message) {
        if (writer == null) {
            openFile();
        }
        String prefix = HeartBeat.vm_name + " " + dateFormat.format(new Date()) + " (" + HeartBeat.id + ") : ";
        try {
            writer.write(prefix + message + "\n");
            writer.flush();
        } catch (IOException e) {
            System.err.println("Error writing to log file.");
        }
    }
}
