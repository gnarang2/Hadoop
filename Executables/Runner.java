public class Runner {

    public static void main(String[] args) {
        try{
            Process ps = new ProcessBuilder("javac", "sub.java").start();
            ps.waitFor();
            System.out.println(ps.exitValue());
            // // Exit value = 1 if unsuccesful else 0
            ps = new ProcessBuilder("jar", "cfe", "program.jar", "sub", "sub.class", "FileOperations.class").start();
            ps.waitFor();
            System.out.println(ps.exitValue());
            ps = new ProcessBuilder("java","-jar","program.jar", ).start();
            // ps.waitFor();
            // System.out.println(ps.exitValue());
            // // Process ps2 = Runtime.getRuntime().exec(new String[]{"javac", "sub.java"});
            // // ps2.waitFor();
            // // Process ps1 = Runtime.getRuntime().exec(new String[]{"jar", "cfe", "sub.jar", "sub", "sub.class"});
            // // ps1.waitFor();
            // // Process ps=Runtime.getRuntime().exec(new String[]{"java","-jar","sub.jar","hi", "hello"});
            // // ps.waitFor();
            // InputStream is=ps.getInputStream();
            // byte b[]=new byte[is.available()];
            // is.read(b,0,b.length);
            // System.out.println(new String(b));
            // System.out.println(ps.exitValue());
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
