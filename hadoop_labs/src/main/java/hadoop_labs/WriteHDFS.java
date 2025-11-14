package hadoop_labs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        if(args.length < 2) {
            System.out.println("Usage: WriteHDFS <hdfs_file_path> <message>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(args[0]);

        if(!fs.exists(filePath)) {
            FSDataOutputStream outStream = fs.create(filePath);
            outStream.writeUTF(args[1]);
            outStream.close();
            System.out.println("File created: " + filePath);
        } else {
            System.out.println("File already exists: " + filePath);
        }

        fs.close();
    }
}
