package parallelDistributed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alvaro on 27/09/17.
 */
public class Cluster implements Serializable {
    //private static final long serialVersionUID = 1L;
    public List<Point> points;
    public Point centroid;
    public int id;

    //Creates a new Cluster
    public Cluster() {
        this.id = 0;
        this.points = new ArrayList();
        this.centroid = null;
    }

   /* public void setPoints(HazelcastInstance instance){
        points= instance.getList(String.valueOf(id));
    }*/

    public List getPoints() {
        return points;
    }

    public void addPoint(Point point) {
        points.add(point);
    }

    public void setPoints(List points) {
        this.points = points;
    }

    public Point getCentroid() {
        return centroid;
    }

    public void setCentroid(Point centroid) {
        this.centroid = centroid;
    }

    public int getId() {
        return id;
    }

    public void clear() {
        points.clear();
    }
    public void setID(int id) {
        this.id = id;
    }

    public void plotCluster() {
        System.out.println("[Cluster: " + id+"]");
        System.out.println("[Centroid: " + centroid + "]");
        System.out.println("[Points: \n");
        for(Point p : points) {
            System.out.println(p);
        }
        System.out.println("]");
    }
/*
    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        points.forEach(point -> {
            try {
                point.writeExternal(objectOutput);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        centroid.writeExternal(objectOutput);
        objectOutput.writeInt(id);
       /* public List<Point> points;
        public Point centroid;
        public int id; *//*
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        points = new ArrayList();
        int count = objectInput.readInt();
        for (int i = 0; i < count; i++) {
            Point point = new Point();
        }
        centroid.readExternal(objectInput);
        id = objectInput.readInt();
    }
    */
}
