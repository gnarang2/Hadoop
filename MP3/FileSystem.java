import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;

public class FileSystem {
    
    public String path;

    public static String changeFile(String fileName){
        return fileName.replace('*', '-');
    }

    public FileSystem(String path){
        this.path = path;
    }

    public static void setUp(){

        File dir = new File(DistributedFileSystem.DataNodePath);
        dir.mkdirs();
        for(File files: dir.listFiles()){
            files.delete();
        }
        setUpExecutablesFolder(DistributedFileSystem.DataNodePath);
        dir = new File(Client.ClientPath);
        dir.mkdirs();
        setUpExecutablesFolder(Client.ClientPath);
        
    }

    public boolean fileExists(String fileName){
        File myObj = new File(path + fileName);
        if(!myObj.exists()){
            return false;
        }
        return true;
    }

    public boolean storeDataOffset(byte[] contents, String fileName, int Offset) {
        if(Offset == 0){
            completeDelete(fileName);
        }
        try{
            File file = new File(path + fileName);
            OutputStream os = new FileOutputStream(file, true);
            os.write(contents);
            os.close();
        }catch(Exception e){
            return false;
        }
        return true;
    }
    
    
    public boolean storeData(byte[] contents, String fileName) {
        completeDelete(fileName);
        try{
            File file = new File(path + fileName);
            OutputStream temp = new FileOutputStream(file); 
            BufferedOutputStream os = new BufferedOutputStream(temp);
            

            os.write(contents);
            os.close();
        }catch(Exception e){
            return false;
        }
        return true;
    }

    public long getFileSize(String fileName){
        if(fileExists(fileName)){
            File file = new File(path + fileName);
            return file.length();
        }
        return 0;
    }

    public static long getFileSizeForDemo(String fileName){
        File file = new File("Client/" + fileName);
        return file.length();
    }



    // return file data upto certain offset
    public byte[] getFileData(String fileName, Integer offset) {

        // Check if file in local directory
        byte[] contents = new byte[0];
        File file = new File(path + fileName);
        FileInputStream fis;
        int bytesRead = 0;

        try {
            fis = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            return contents;
        }

        BufferedInputStream bis = new BufferedInputStream(fis);
        contents = new byte[Messenger.MAX_MESSAGE_SIZE];
        try {
            Integer size = Math.min(Messenger.MAX_MESSAGE_SIZE, (int)file.length() - offset);
            bis.skip((long)offset);
            bytesRead = bis.read(contents, 0, size);
        } catch (IOException e) {
            bytesRead = 0;
            contents = new byte[0];
        }

        try {
            fis.close();
            bis.close();
        } catch (IOException e) {
            bytesRead = 0;
            contents = new byte[0];
            e.printStackTrace();
            System.out.println("\n Enter a command > ");
        }
        return Arrays.copyOf(contents, bytesRead);
    }

    public void completeDelete(String fileName){
        File myObj = new File(path + fileName);
        if(myObj.exists()){
            myObj.delete();
        }
    }

    public static void deleteFile(String fileName){
        File myObj = new File(fileName);
        if(myObj.exists()){
            myObj.delete();
        }
    }

    public static void setUpExecutablesFolder(String path){
        File myObj = new File(path + "Executables");
        if(!myObj.exists()){
            myObj.mkdir();
            return;
        }
        for (File subFile : myObj.listFiles()) {
            subFile.delete();
        }
    }

    public String getExecutablePath(){
        return path + "Executables";
    }

    public String generateExecutablesFolderFileName(String fileName){
        return String.join("", "Executables/", fileName);
    }
    
    public boolean checkExecutableFolder(String fileName){
        if(fileExists(generateExecutablesFolderFileName(fileName))){
            return true;
        }
        return false;
    }

    public boolean checkExecutableFolderFile(String fileName){
        File dir = new File(getExecutablePath());
        for(File file: dir.listFiles()){
            if(file.getName().substring(0, fileName.length()).equalsIgnoreCase(fileName)){
                return true;
            }
        }
        return false;
    }

    public String getExecutablePath(String fileName){
        return String.join("", this.path,"Executables/",fileName);
    }

    public void cleanUpExecutablesFolder(){
        File execPath = new File(getExecutablePath());
        for(File files: execPath.listFiles()){
            files.delete();
        }
    }


    public String getDivisions(Integer numDivisions, String fileName){
        
        ArrayList<Long> partitions = new ArrayList<>();
        
        FileInputStream fis;
        BufferedInputStream bis;
        File file = new File(path + fileName);
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
                if(flag == 1 && var.equalsIgnoreCase("\n")){
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


}
