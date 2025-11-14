package hadoop_labs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        FileSystem fs;

        try {
            fs = FileSystem.get(conf);

            if (args.length < 3) {
                System.out.println("Usage: chemin_fichier nom_fichier nouveau_nom_fichier");
                System.exit(1);
            }

            String path = args[0];
            String filename = args[1];
            String newname = args[2];

            Path filepath = new Path(path, filename);

            if (!fs.exists(filepath)) {
                System.out.println("File does not exist: " + filepath);
                System.exit(1);
            }

            FileStatus infos = fs.getFileStatus(filepath);

            System.out.println("File Size: " + infos.getLen());
            System.out.println("File Owner: " + infos.getOwner());
            System.out.println("Permissions: " + infos.getPermission());
            System.out.println("Replication: " + infos.getReplication());
            System.out.println("Block Size: " + infos.getBlockSize());

            BlockLocation[] blocks = fs.getFileBlockLocations(infos, 0, infos.getLen());
            for (BlockLocation block : blocks) {
                System.out.println("Block offset: " + block.getOffset());
                System.out.println("Block length: " + block.getLength());
            }

            fs.rename(filepath, new Path(path, newname));
            System.out.println("Renamed to: " + newname);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
