import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;

public class FileOperations {

    private String input;
    private String output;
    private String machineNumber;
    private Long start;
    private Long end;
    private FileInputStream fis = null;
    private BufferedInputStream bis = null;
    private Long currentPointer;


    public FileOperations(String fileIn, String fileOut, String machineID, Long start, Long end) {
        this.input = fileIn;
        this.output = fileOut;
        this.start = start;
        this.currentPointer = this.start;
        this.end = end;
        this.machineNumber = machineID;
        openFile();
    }

    public boolean openFile(){
        if(fis != null && bis != null){
            return true;
        }
        File file = new File(this.input);
        try {
            fis = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            return false;
        }
        bis = new BufferedInputStream(fis);
        try {
            bis.skip(this.start);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public String[] getFileData() {

        Integer length = 20;
        String[] data = new String[length];
        if(!openFile()){
            return data;
        }
        Integer currentLines = 0;
        byte[] temp = new byte[1];
        String var = new String("");
        int n = 0;
        while(length > 0 && this.currentPointer <= this.end){
            try {
                n = bis.read(temp);
            } catch (IOException e) {
                return new String[0];
            }
            if(n == -1){
                break;
            }
            this.currentPointer += 1L;
            String currentChar = new String(temp);
            char c = currentChar.charAt(0);
            System.out.println((int)c);
            if(currentChar.equalsIgnoreCase("\n")){
                data[currentLines] = var;//.substring(0,var.length()-1);
                var = new String("");
                currentLines += 1;
                length -= 1;
                continue;
            }
            var += currentChar;
        }

        if(currentPointer == end){
            try {
                bis.close();
                fis.close();
            } catch (IOException e) {
            }
        }

        return Arrays.copyOfRange(data, 0, currentLines);
        
        // byte[] contents = new byte[0];
        // File file = new File(this.input);
        // FileInputStream fis;
        // int bytesRead = 0;
        // String[] data = new String[0];

        // try {
        //     fis = new FileInputStream(file);
        // } catch (FileNotFoundException e) {
        //     return data;
        // }

        // BufferedInputStream bis = new BufferedInputStream(fis);
        // contents = new byte[end-start];
        // try {
        //     bis.skip((long)start);
        //     bytesRead = bis.read(contents, start, end-start);
        // } catch (IOException e) {
        //     bytesRead = 0;
        //     contents = new byte[0];
        // }

        // try {
        //     fis.close();
        //     bis.close();
        // } catch (IOException e) {
        //     bytesRead = 0;
        //     contents = new byte[0];
        // }
        // if(contents.length == 0){
        //     return data;
        // }
        // contents = Arrays.copyOf(contents, bytesRead);
        // String temp = new String(contents);
        // data = temp.split("\n");
        // return data;
    }

    // public String convertBytesToString(byte[] data) {
    //     return null;
    // }

    // public byte[] convertStringToBytes(String data) {
    //     return null;
    // }

    public void createOutputFile(String fileName) {
        File file = new File(fileName);
        try {
            file.createNewFile();
        } catch (IOException e) {
        }
    }

    public void appendFileData(String data, String key){
        String out = createPreMappleOutputFileName(key);
        if(checkFileExists(out)){
            createOutputFile(out);    
        }

        // System.out.println("Data appending is:" + data);
        File file = new File(out);
        FileWriter fr;
        try {
            fr = new FileWriter(file, true);
            BufferedWriter br = new BufferedWriter(fr);
            br.write(data);
    
            br.close();
            fr.close();
        } catch (IOException e) {
        }
 
    }

    public boolean checkFileExists(String fileName){
        File file = new File(fileName);
        if (file.exists()) {
            return true;
        }
        return false;
        
    }

    public String createPreMappleOutputFileName(String key){
        return String.join("_", this.output, this.machineNumber, key) + ".txt";
    }




}
