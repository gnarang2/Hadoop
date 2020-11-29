import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

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

    public static void juiceConsolidate(HashMap<String, FileOperations> objects){
        
        String[] temp = new String[0];
        Integer maxCount = 0;
        ArrayList<String> outputs = new ArrayList<>();
        FileOperations myObj = null;

        for(String s: objects.keySet()){
            temp = objects.get(s).getFileData();
            Integer currentCount = Integer.parseInt(temp[0]);
            if(maxCount == currentCount){
                outputs.add(s);
                myObj = objects.get(s);
                maxCount = currentCount;
            } else if(currentCount > maxCount){
                outputs.clear();
                outputs.add(s);
                myObj = objects.get(s);
            }
        }

        for(String s: outputs){
            myObj.appendFileData(s + "\n");
        }
    }

    public static void main(String[] args){
        
        String var = args[0];
        if(var.equalsIgnoreCase("0")){
            String inputFile = args[1];
            String outputFile = args[2];
        
            FileOperations obj = new FileOperations(inputFile, outputFile);
            juice(obj);
        } else {
            String outputFile = args[1];
            HashMap<String, FileOperations> objects = new HashMap<>();
            String[] files = args[2].split("\\|");
            for(String s: files){
                objects.put(s, new FileOperations(outputFile + "_" + s + ".txt", outputFile));
            }
            juiceConsolidate(objects);
        }
        
        
        
    }
}