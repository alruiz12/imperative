package parallelDistributed;

import com.hazelcast.core.HazelcastInstance;

import java.io.*;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by alvaro on 27/09/17.
 */
public class Point implements Serializable {
    private double x = 0;
    private double y = 0;
    private int cluster_number = 0;
    //private static final long serialVersionUID = 1;



    public void setX(double x) {
        this.x = x;
    }

    public double getX()  {
        return this.x;
    }

    public void setY(double y) {
        this.y = y;
    }

    public double getY() {
        return this.y;
    }

    public void setCluster(int n) {
        this.cluster_number = n;
    }

    public int getCluster() {
        return this.cluster_number;
    }

    //Calculates the distance between two points.
    protected static double distance(Point p, Point centroid) {
        return Math.sqrt(Math.pow((centroid.getY() - p.getY()), 2) + Math.pow((centroid.getX() - p.getX()), 2));
    }

    //Creates random point
    protected static Point createRandomPoint(int min, int max) {
        Random r = new Random();
        double x = min + (max - min) * r.nextDouble();
        double y = min + (max - min) * r.nextDouble();
        Point point = new Point();
        point.setX(x);
        point.setY(y);
        return point;
    }

    protected static ConcurrentMap<Integer,Point> createRandomPoints(int min, int max, int number, HazelcastInstance instance) {
        ConcurrentMap<Integer,Point> points = instance.getMap("points");
        for(int i = 0; i<number; i++) {
            Point aux = createRandomPoint(min,max);
            points.put(i,aux);
        }
        return points;
    }

    public String toString() {
        return "("+x+","+y+")";
    }
/*
    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeDouble(x);
        objectOutput.writeDouble(y);
        objectOutput.writeInt(cluster_number);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        x=objectInput.readDouble();
        y=objectInput.readDouble();
        cluster_number= objectInput.readInt();
    } */
}
