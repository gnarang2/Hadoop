import java.util.HashMap;

public class DistributedFileSystem {

    protected static HashMap<String, FileInformation> localFiles = new HashMap<String, FileInformation>();
    public static Integer MAX_FILE_SIZE = Messenger.MAX_MESSAGE_SIZE;
    public static final String DataNodePath = "DataNode/";

    public static FileSystem DataNodeFileSystem = new FileSystem(DataNodePath);

    public static class FileInformation {
        public FileInformation(String name, Integer updateCount, boolean master) {
            this.name = name;
            this.updateCount = updateCount;
            this.status = master ? Commands.WRITING : "";
        }
        public FileInformation(String name){
            this.name = name;
        }
        
        public String toString(){
            String returnString = this.name + ":" + Integer.toString(this.updateCount) + ":" + this.status;
            return returnString;
        } 

        String name = "";
        Integer updateCount = 1;
        volatile String status = "";
    }

    public static void updateCount(String name, Integer count) { // 1 or non 1
        if (localFiles.containsKey(name)) {
            FileInformation file = localFiles.get(name);
            if(count != 1){
                file.updateCount = file.updateCount < count ? count : file.updateCount + 1;
            }
        }
    }

    public static void updateCount(String name) {
        if (localFiles.containsKey(name)) {
            FileInformation file = localFiles.get(name);
            file.updateCount += 1;
        }
    }

    public static Integer getCount(String name) {
        if(localFiles.containsKey(name)){
            return localFiles.get(name).updateCount;
        } else {
            return 0;
        }
        
    }

    private static boolean storeData(byte[] contents, String fileName) {
        return DataNodeFileSystem.storeData(contents, fileName);
    }

    public static long getFileSize(String fileName){
        return DataNodeFileSystem.getFileSize(fileName);
    }

    public static byte[] getFileInfo(String fileName){
        if(fileExists(fileName) && localFiles.containsKey(fileName)){
            return (Integer.toString( (int)getFileSize(fileName) ) + '|' + Integer.toString(localFiles.get(fileName).updateCount)).getBytes();
        }
        return "".getBytes();
    }

    // return file data upto certain offset
    public static byte[] getFileData(String fileName, Integer offset) {
        return DataNodeFileSystem.getFileData(fileName, offset);
    }

    private static boolean storeFile(byte[] file, String fileName){
        if(localFiles.containsKey(fileName)){
            completeDelete(fileName);
        }
        if(!storeData(file, fileName)){
            return false;
        }
        return true;
    }

    private static void deleteFileDFS(String fileName){
        localFiles.remove(fileName);
    }

    private static void completeDelete(String fileName){
        DataNodeFileSystem.completeDelete(fileName);
    }

    public static void deleteFile(String fileName){
        deleteFileDFS(fileName);
        // Make changes
        completeDelete(fileName);
    }

    public static boolean fileExists(String fileName){
        return DataNodeFileSystem.fileExists(fileName);
    }

    public static boolean fileFree(String fileName){
        if(localFiles.containsKey(fileName) && localFiles.get(fileName).status == Commands.READABLE){
            return true;
        } else if (!localFiles.containsKey(fileName)){
            return true;
        }
        return false;
    }

    public static void markWriteBusy(String fileName){
        if(localFiles.containsKey(fileName)){
            localFiles.get(fileName).status = Commands.WRITING;    
        }
    }

    public static void unmarkwriteBusy(String fileName){
        if(localFiles.containsKey(fileName)){
            localFiles.get(fileName).status = Commands.READABLE;    
        }
    }

    // USED FOR PUTTING FILE, THIS DATA NODE PUTTING
    public static boolean putFile(String fileName, byte[] contents, Integer updateCount){
        // if(localFiles.containsKey(fileName) && updateCount != 1){            
        //     if(updateCount < getCount(fileName)){  //if updateCount 1 file coming for the first time. SOME POTENTIAL ERROR
        //         localFiles.get(fileName).status = Commands.READABLE;
        //         return true; // you have to let master know file is readable.
        //     }
        // }
        // if(!storeFile(contents, fileName)){
        //     completeDelete(fileName);
        //     return false;
        // }
        if(!localFiles.containsKey(fileName)){
            FileInformation newFile = new FileInformation(fileName);
            localFiles.put(fileName, newFile);
            localFiles.get(fileName).updateCount = updateCount;
            localFiles.get(fileName).status = Commands.READABLE;
        } else {
            updateCount(fileName, updateCount);
        }
        return true;
    }

    // Used for both writing and replicating, THIS DATANODE WRITING
    // public static boolean updateFile(String fileName, byte[] contents, Integer updateCount){
    //     if(storeFile(contents, fileName, updateCount)){
    //         localFiles.get(fileName).status = Commands.READABLE;
    //         return true;
    //     } else {
    //         return false;
    //     }
        
    // }

    public static FileInformation createFileInformation(String name, Integer updateCount) {
        FileInformation fileInfo = new FileInformation(name, updateCount, true);
        return fileInfo;
    }
    
    public static FileInformation createFileInformationReplica(String name, Integer updateCount) {
        FileInformation fileInfo = new FileInformation(name, updateCount, false);
        fileInfo.status = Commands.REPLICATING;
        return fileInfo;
	}

	public static String getStatus(String fileName) {
        if(localFiles.containsKey(fileName)){
            return localFiles.get(fileName).status;
        }
		return "";
	}

}
