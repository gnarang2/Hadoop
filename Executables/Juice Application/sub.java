import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class sub {

    public static void juice(FileOperations myObj){
        String[] temp = new String[0];
        Integer count = 0;
        while(true){
            temp = myObj.getFileData();
            if(temp.length < 20){
                count+=temp.length;
                break;
            }
            count+=temp.length;
        }
        myObj.appendFileData(Integer.toString(count));
    }

    public static void main(String[] args){
        
        String inputFile = args[0];
        String outputFile = args[1];
    
        FileOperations obj = new FileOperations(inputFile, outputFile);
        juice(obj);
        
        // FileOperations obj = new FileOperations(inputFile, outputFile, machineNumber, startOff, endOff);
        
    }
}