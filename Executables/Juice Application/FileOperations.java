import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class FileOperations {

    private String input;
    private String output;
    private FileReader fr = null;
    private BufferedReader br = null;

    public FileOperations(String fileIn, String fileOut) {
        this.input = fileIn;
        this.output = fileOut;
        openFile();
    }

    public boolean openFile(){
        if(fr != null && br != null){
            return true;
        }
        try{
            File file = new File(this.input);
            fr = new FileReader(file);
            br = new BufferedReader(fr);
        } catch (IOException e){
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
        String var = new String("");
        int n = 0;
        String line;
        while(length > 0){
            try {
                line = br.readLine();
            } catch (IOException e) {
                return new String[0];
            }
            if(line == null){
                try{
                    br.close();
                    fr.close();
                } catch (IOException e){

                }
                break;
            }
            data[currentLines] = line;
            currentLines += 1;
            length--;
        }

        return Arrays.copyOfRange(data, 0, currentLines);
    }

    public void createOutputFile(String fileName) {
        File file = new File(fileName);
        try {
            file.createNewFile();
        } catch (IOException e) {
        }
    }

    public void appendFileData(String data){
        String out = createPreMappleOutputFileName();
        if(checkFileExists(out)){
            createOutputFile(out);    
        }

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

    public String createPreMappleOutputFileName(){
        return this.output + ".txt";
    }




}
