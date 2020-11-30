public class Runner {

    public static void main(String[] args) {
        try{
            Process ps = new ProcessBuilder("javac", "sub.java").start();
            ps.waitFor();
            System.out.println(ps.exitValue());
            ps = new ProcessBuilder("jar", "cfe", "characterCountJuice.jar", "sub", "sub.class", "FileOperations.class").start();
            ps.waitFor();
            System.out.println(ps.exitValue());
            ps = new ProcessBuilder("cp", "characterCountJuice.jar", "../../MP3/Client").start();
            ps.waitFor();
            System.out.println(ps.exitValue());
            // ps = new ProcessBuilder("java", "-jar", "juice1.jar","1", "juiceOutput", "aa|bb").start();
            // ps.waitFor();
            // System.out.println(ps.exitValue());

            
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
