import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

public class FileSystem {
    
    public String path;

    public static String changeFile(String fileName){
        return fileName.replace('/','-');
    }

    public FileSystem(String path){
        this.path = path;
        File dir = new File(this.path);
        if(!dir.exists()){
            dir.mkdir();
        } else if(dir.exists() && this.path == DistributedFileSystem.DataNodePath){
            for(File files: dir.listFiles()){
                files.delete();
            }
        }
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
            // if(path == "Client/"){
            // }
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



}