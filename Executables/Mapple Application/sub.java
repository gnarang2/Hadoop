import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class sub {

    public static void mapple(FileOperations myObj) {
        String[] temp = new String[0];
        while(true){
            temp = myObj.getFileData();
            if(temp.length == 0){
                break;
            }
            for(String pair: temp){
                myObj.appendFileData(new String(pair + " 1\n"), pair);
            }
        }
    }

    public static void main(String[] args){
        
        String inputFile = args[0];
        String outputFile = args[1];
        String machineNumber = args[2];
        Long startOff = Long.parseLong(args[3]);
        Long endOff = Long.parseLong(args[4]);
        // String[] val = getDivisions(4, "test.txt").split(":");
        // for(int i = 0; i < val.length; i+=2){
        //     System.out.println("new iteration");
        //     System.out.println(i);
        //     System.out.println(val[i]);
        //     System.out.println(val[i+1]);
        //     FileOperations obj = new FileOperations("test.txt", "output", "XYZ", Long.parseLong(val[i]), Long.parseLong(val[i+1]));
        //     mapple(obj);
        // }

        FileOperations obj = new FileOperations(inputFile, outputFile, machineNumber, startOff, endOff);
        mapple(obj);
        
        // FileOperations obj = new FileOperations(inputFile, outputFile, machineNumber, startOff, endOff);
        
    }
}