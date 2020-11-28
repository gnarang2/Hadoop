public class Runner {

    public static void main(String[] args) {
        try{
            Process ps = new ProcessBuilder("javac", "sub.java").start();
            ps.waitFor();
            System.out.println(ps.exitValue());
            ps = new ProcessBuilder("jar", "cfe", "juice1.jar", "sub", "sub.class", "FileOperations.class").start();
            ps.waitFor();
            System.out.println(ps.exitValue());
            ps = new ProcessBuilder("cp", "juice1.jar", "../../MP3/Client").start();
            ps.waitFor();
            System.out.println(ps.exitValue());

            
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
