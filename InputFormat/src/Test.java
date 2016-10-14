/**
 * Created by nina on 03.10.16.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;


import java.util.zip.Inflater;

public class Test {
    public static void main(String[] args) {
        Path path_file = new Path(args[1]);
        Path path_idx = new Path(args[2]);

        FileSystem fileSystem = new FileSystem(new Configuration());

        if (fileSystem.exists(path_file)) {
            System.out.println("File " + args[1]+ " exists");
        } else {
            System.out.println("File " + args[1]+ "don't exists");
            return;
        }

        if (fileSystem.exists(path_idx)) {
            System.out.println("File " + args[2]+ " exists");
        } else {
            System.out.println("File " + args[2]+ "don't exists");
            return;
        }


    }
}
