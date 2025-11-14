package hadoop_labs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        // Vérifie que l'utilisateur a passé un paramètre (nom du fichier HDFS)
        if (args.length < 1) {
            System.out.println("Usage: ReadHDFS <hdfs-file-path>");
            System.exit(1);
        }

        // Chemin complet du fichier HDFS passé en paramètre
        String hdfsFilePath = args[0];
        Path filePath = new Path(hdfsFilePath);

        // Configuration Hadoop
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Vérifie si le fichier existe
        if (!fs.exists(filePath)) {
            System.out.println("File does not exist: " + hdfsFilePath);
            fs.close();
            System.exit(1);
        }

        // Ouvre le fichier en lecture
        FSDataInputStream inStream = fs.open(filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

        System.out.println("=== Contenu du fichier " + hdfsFilePath + " ===");
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

        // Ferme les flux et le FileSystem
        br.close();
        inStream.close();
        fs.close();
    }
}

