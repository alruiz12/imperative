package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by alvaro on 27/09/17.
 */
public class Main {
    public static void main(String[] args) {
        Cluster cluster = new Cluster(1);
        Point point = new Point(1.0,2.0);
        List<Point> points = point.createRandomPoints(0,100,5);
        cluster.setPoints(points);
        cluster.plotCluster();
        String s = null;
        String[] cmd = {
                "/bin/bash",
                "-c",
                "python /home/alvaro/imperative/src/main/java/kmeans/script.py 5 0 1 2 3 4"
        };
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));

            BufferedReader stdError = new BufferedReader(new
                    InputStreamReader(p.getErrorStream()));

            // read the output from the command
            System.out.println("Here is the standard output of the command:\n");
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
