import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class sub {

    public static void mapple(FileOperations myObj){
        String[] temp = new String[0];
        while(true){
            temp = myObj.getFileData();
            if(temp.length == 0){
                break;
            }
            for(String pair: temp){
                System.out.println(pair);
                myObj.appendFileData(new String(pair + " 1\n"), pair);
            }
        }
    }

    public static String getDivisions(Integer numDivisions, String fileName){
        
        ArrayList<Long> partitions = new ArrayList<>();
        
        FileInputStream fis;
        BufferedInputStream bis;
        File file = new File(fileName);
        try {
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);
        } catch (FileNotFoundException e) {
            return new String();
        }
        
        Long length = file.length();
        Long divisionLength = length/numDivisions;
        Long currentLength = 0L;
        byte[] temp = new byte[1];
        partitions.add(0L);
        Integer flag = 0;
        Long offset = 0L;
        try {
            while (bis.read(temp) >= 1) {
                currentLength += 1L;
                offset += 1L;
                if(currentLength.equals(divisionLength)){
                    flag = 1;
                }
                String var = new String(temp);
                // System.out.println((int)var.charAt(0));
                if(flag == 1 && var.equalsIgnoreCase("\n")){
                    // System.out.println("Adding offset");
                    flag = 0;
                    currentLength = 0L;
                    partitions.add(offset);
                    partitions.add(offset);
                }
            }
        } catch (IOException e) {
            return new String();
        }
        partitions.add(file.length());

        String response = "";
        for(int i = 0; i < partitions.size(); i++){
            response += Long.toString(partitions.get(i)) + ":"; 
        }

        try {
            fis.close();
            bis.close();
        } catch (IOException e) {
        }
        
        return response.substring(0, response.length()-1);

    }

    public static void main(String[] args){
        
        // String inputFile = args[0];
        // String outputFile = args[1];
        // String machineNumber = args[2];
        // Long startOff = Long.parseLong(args[3]);
        // Long endOff = Long.parseLong(args[4]);
        String[] val = getDivisions(4, "test.txt").split(":");
        for(int i = 0; i < val.length; i+=2){
            System.out.println("new iteration");
            System.out.println(i);
            System.out.println(val[i]);
            System.out.println(val[i+1]);
            FileOperations obj = new FileOperations("test.txt", "output", "XYZ", Long.parseLong(val[i]), Long.parseLong(val[i+1]));
            mapple(obj);
        }
        
        // FileOperations obj = new FileOperations(inputFile, outputFile, machineNumber, startOff, endOff);
        
        // System.out.println(getDivisions(4, "test.txt"));
        
    }
}