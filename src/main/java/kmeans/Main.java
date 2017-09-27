package kmeans;

import java.util.List;

/**
 * Created by alvaro on 27/09/17.
 */
public class Main {
    public static void main(String[] args) {
        Cluster cluster = new Cluster(1);
        Point point = new Point(1.0,2.0);
        List<Point> points = point.createRandomPoints(0,100,55);
        cluster.setPoints(points);
        cluster.plotCluster();
    }


}
