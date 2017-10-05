package parallelDistributed;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


//import kmeansOO.Point;

/**
 * Created by alvaro on 27/09/17.
 */
public class KMeans {
    /*
    //Number of Clusters. This metric should be related to the number of points
    private int NUM_CLUSTERS = 100;
    //Number of Points
    private int NUM_POINTS = 1000000;
    //Min and Max X and Y
    private static final int MIN_COORDINATE = 0;
    private static final int MAX_COORDINATE = 10000;
*/
    private List<Point> points;         // Todo: distribute it
    //private List<Cluster> clusters;

    public KMeans() {
        this.points = new ArrayList();
        //this.clusters = new ArrayList();
    }


    public static void end(List<Cluster> clusters){
        for (int i = 0; i < clusters.size(); i++) {
            try {
                System.out.println(clusters.get(i).id);
                PrintWriter writer = new PrintWriter(String.valueOf(clusters.get(i).id) , "UTF-8") ;
                for (int j = 0; j < clusters.get(i).getPoints().size(); j++) {
                    writer.write(String.valueOf(clusters.get(i).getPoints().get(j))); //print();
                }
                writer.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        String s = null;
        String[] cmd = {
                "/bin/bash",
                "-c",
                "python /home/alvaro/imperative/src/main/java/kmeansOO/script.py "+clusters.size()
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

    //Initializes the process
    public static Map init(int numClusters, int minCoordinate, int maxCoordinate, HazelcastInstance instance) {
        //Create Clusters
        Map<Integer, Cluster> clusters = instance.getMap("clusters");
        //Set Random Centroids
        for (int i = 0; i < numClusters; i++) {
            Cluster cluster = new Cluster(i);
            Point centroid = Point.createRandomPoint(minCoordinate,maxCoordinate);
            cluster.setCentroid(centroid);
            clusters.put(i, cluster);
        }
        return clusters;

    }

    //The process to calculate the K Means, with iterating method.
    public static void calculate(Map clusters, Map points, int clustersPart, int pointsPart, long localCount) {
        boolean finish = false;
        int iteration = 0;

        // Add in new data, one at a time, recalculating centroids with each new one.
        while(!finish) {
            //Clear cluster state
            clearClusters(clusters, clustersPart, localCount);
/*
            List<Point> lastCentroids = getCentroids(clusters);

            //Assign points to the closer cluster
            assignCluster(clusters, points);

            //Calculate new centroids.
            calculateCentroids(clusters);

            iteration++;

            List<Point> currentCentroids = getCentroids(clusters);

            //Calculates total distance between new and old Centroids
            AtomicDouble distance = new AtomicDouble();
            for(int i = 0; i < lastCentroids.size(); i++) {         // Todo: can be divided in n processes
                distance.getAndAdd(Point.distance(lastCentroids.get(i),currentCentroids.get(i)));
            }

            if (distance.compareAndSet(0.0,0.0)){
                finish = true;
            }
            */
            finish=true;
        }

    }

    private static void clearClusters(Map clusters, int clustersPart, long localCount) {

        for (int i = 0; i <clustersPart ; i++) {
            Cluster c = (Cluster) clusters.get(i);
            c.clear();

        }
/*
        for(Cluster cluster : clusters) {
            cluster.clear();
        }
*/
    }

    private static List<Point> getCentroids(List<Cluster> clusters) {
        List<Point> centroids = new ArrayList(clusters.size());
        for(Cluster cluster : clusters) {
            Point aux = cluster.getCentroid();
            Point point = new Point(aux.getX(),aux.getY());
            centroids.add(point);
        }
        return centroids;
    }

    private static void assignCluster(List<Cluster> clusters, List<Point> points) {
        double max = Double.MAX_VALUE;
        double min = max;
        int cluster = 0;
        double distance = 0.0;

        for(Point point : points) {
            min = max;
            for(int i = 0; i < clusters.size(); i++) {             // Todo: parallelize inner for's
                Cluster c = (Cluster) clusters.get(i);
                distance = Point.distance(point, c.getCentroid());
                if(distance < min){
                    min = distance;
                    cluster = i;
                }
            }
            point.setCluster(cluster);
            clusters.get(cluster).addPoint(point);
        }
    }

    private static void calculateCentroids(List<Cluster> clusters) {
        for(Cluster cluster : clusters) {           // parallelized
            double sumX = 0;
            double sumY = 0;

            List<Point> list = cluster.getPoints();
            int n_points = list.size();

            for(Point point : list) {               // Todo: can be divided in n processes
                sumX += point.getX();
                sumY += point.getY();
            }

            Point centroid = cluster.getCentroid();
            if(n_points > 0) {
                double newX = sumX / n_points;
                double newY = sumY / n_points;

                centroid.setX(newX);
                centroid.setY(newY);
            }
            };

        }

    public static void run(int numClusters, int num_points, int minCoordinate, int maxCoordinate, int numIter, int numNodes) {
        long startTime;
        long finalTime;

        long parallelTime=0;

        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        Map points = Point.createRandomPoints(minCoordinate, maxCoordinate, num_points, instance);
        Map clusters = init(numClusters, minCoordinate, maxCoordinate, instance);

        IAtomicLong count = instance.getAtomicLong("count");
        long localCount = count.incrementAndGet();
        if (localCount>numNodes){
            // Todo: create distributed long for numNodes and update it as needed
            System.out.println("number of nodes increased");
        }

        int pointsPart = points.size()/numNodes;
        int clustersPart = clusters.size()/numNodes;
        System.out.println(pointsPart+" "+clustersPart);
        calculate(clusters, points, clustersPart, pointsPart, localCount);
        /*
        List<Cluster> clustersOriginal = (List<Cluster>) DeepCopy.copy(clusters);
        List<Point> pointsOriginal = (List<Point>) DeepCopy.copy(points);


        startTime = System.currentTimeMillis();
        calculate(clusters, points);
        finalTime = System.currentTimeMillis();
        System.out.println(finalTime-startTime);
        parallelTime += (finalTime - startTime);

        //end(clusters);


        System.out.println("Parallel time: "+parallelTime + " ms");
        */
    }

    public static void runSecondary(int numClusters, int num_points, int minCoordinate, int maxCoordinate, int numIter, int numNodes) {
        long startTime;
        long finalTime;

        long parallelTime=0;
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();

        Map points = instance.getMap("points");
        System.out.println(points.size());

        Map clusters = instance.getMap("clusters");
        System.out.println(clusters.size());

        int pointsPart = points.size()/numNodes;
        int clustersPart = clusters.size()/numNodes;
        calculate(clusters, points, clustersPart, pointsPart, localCount);

        // Todo: when calling clear, do from end to beggining, or even clearer from x to y, what happens when is and odd number?

        //instance.
        /*
        List<Point> points = Point.createRandomPoints(minCoordinate, maxCoordinate, num_points);
        List<Cluster> clusters = init(numClusters, minCoordinate, maxCoordinate);

        List<Cluster> clustersOriginal = (List<Cluster>) DeepCopy.copy(clusters);
        List<Point> pointsOriginal = (List<Point>) DeepCopy.copy(points);


        startTime = System.currentTimeMillis();
        calculate(clusters, points);
        finalTime = System.currentTimeMillis();
        System.out.println(finalTime-startTime);
        parallelTime += (finalTime - startTime);

        //end(clusters);


        System.out.println("Parallel time: "+parallelTime + " ms");
        */
    }

    }



