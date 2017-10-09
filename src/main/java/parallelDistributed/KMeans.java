package parallelDistributed;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;


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
    public static ConcurrentMap<Integer, Cluster> init(int numClusters, int minCoordinate, int maxCoordinate, HazelcastInstance instance, Map<Integer, Integer> clearIter) {
        //Create Clusters
        ConcurrentMap<Integer, Cluster> clusters = instance.getMap("clusters");
        //Set Random Centroids
        for (int i = 0; i < numClusters; i++) {
            Cluster cluster = new Cluster();
            cluster.setID(i);
            //cluster.clearIter=instance.getAtomicLong("clearIter");
            Point centroid = Point.createRandomPoint(minCoordinate,maxCoordinate);
            cluster.setCentroid(centroid);
            clusters.put(i, cluster);
            System.out.println("init clusters: key: "+i+" ; value: "+clusters.get(i));
            clearIter.put(i,0);
        }
        return clusters;

    }

    //The process to calculate the K Means, with iterating method.
    public static void calculate(ConcurrentMap clusters, ConcurrentMap points, int clustersPart, int pointsPart, long localCount, int numNodes, ConcurrentMap<Integer, Integer> clearIter, HazelcastInstance instance) {
        boolean finish = false;
        long iteration = 1;


        // Add in new data, one at a time, recalculating centroids with each new one.
        while(!finish) {
            //Clear cluster state
            clearClusters(clusters, clustersPart, localCount, numNodes, iteration, clearIter, instance);

            //List<Point> lastCentroids = getCentroids(clusters, clustersPart, localCount, numNodes);

            //Assign points to the closer cluster
            assignCluster(clusters, points, pointsPart, localCount, numNodes, iteration, clearIter); // Todo now!!!!!!!!!!!!
            System.out.println("calculate end (localcount: "+localCount+" )");
/*
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

    private static void clearClusters(Map clusters, int clustersPart, long localCount, int numNodes, long iteration, ConcurrentMap<Integer, Integer> clearIter, HazelcastInstance instance) {
        if (localCount < numNodes){ // if it's not last node
            int clusterIter=0;
            for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart ; i++) {
                // walk through its part
                Cluster c = (Cluster) clusters.get(i);
                clusterIter = clearIter.get(i);
                if (clusterIter < iteration){
                    c.clear();
                    clearIter.replace(i, clusterIter+1) ;
                }
            }

        } else {
            int clusterIter=0;
            int module = clusters.size()%numNodes;
            for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart + module; i++) {
                // walk through its part (including module)
                Cluster c = (Cluster) clusters.get(i);
                clusterIter =  clearIter.get(i);
                if (clusterIter < iteration){
                    c.clear();
                    clearIter.replace(i, clusterIter+1) ;
                }

            }

        }

    }

    private static List<Point> getCentroids(Map clusters, int clustersPart, long localCount, int numNodes) {
        List<Point> centroids = new ArrayList(clusters.size());

        if (clustersPart < numNodes-1){ // if it's not last node

            for (int i = (int) (localCount*numNodes); i <clustersPart*numNodes + clustersPart ; i++) {
                // walk through its part
                Cluster c = (Cluster) clusters.get(i);
                Point point = new Point();
                point.setX(c.getCentroid().getX());
                point.setY(c.getCentroid().getY());
                centroids.add(point);
            }

        } else {

            int module = clusters.size()%numNodes;
            for (int i = (int) (localCount*numNodes); i <clustersPart*numNodes + clustersPart  + module; i++) {
                // walk through its part (including module)
                Cluster c = (Cluster) clusters.get(i);
                Point point = new Point();
                point.setX(c.getCentroid().getX());
                point.setY(c.getCentroid().getY());                centroids.add(point);

            }

        }

        return centroids;
    }


    private static void assignCluster(Map clusters, Map points, int pointsPart, long localCount, int numNodes, long iteration, Map<Integer, Integer> clearIter) {
        double max = Double.MAX_VALUE;
        double min = max;
        int cluster = 0;
        double distance = 0.0;

        if (localCount < numNodes){ // if it's not last node

            for (int i = (int) ((localCount-1)*pointsPart); i <(localCount-1)*pointsPart + pointsPart ; i++) {
                // walk through its part
                Point point = (Point) points.get(i);
                for (int j = 0; j < clusters.size(); j++) {
                    Cluster c = (Cluster) clusters.get(j);
                    System.out.println("assignCluster, clear iter of cluster "+ j +" is "+clearIter.get(j));
                   /* if (c.clearIter.get() == iteration ){
                        distance = Point.distance(point, c.getCentroid());
                        if(distance < min){
                            min = distance;
                            cluster = i;
                        }
                    } else{
                        // clean and update clearIter
                    }*/

                }
                if (distance<max) { // if any point is ready
                    point.setCluster(cluster);
                    Cluster aux = (Cluster) clusters.get(cluster);
                    aux.addPoint(point);
                }

            }

        } else {

            int module = clusters.size()%numNodes;
            for (int i = (int) ((localCount-1)*pointsPart); i <(localCount-1)*pointsPart + pointsPart + module; i++) {
                // walk through its part
                Point point = (Point) points.get(i);
                for (int j = 0; j < clusters.size(); j++) {
                    Cluster c = (Cluster) clusters.get(j);
                    System.out.println("assignCluster, clear iter of cluster "+ j +" is "+clearIter.get(j));


                   /* if (c.clearIter.get() == iteration ){
                        distance = Point.distance(point, c.getCentroid());
                        if(distance < min){
                            min = distance;
                            cluster = i;
                        }
                    } else{
                        // clean and update clearIter
                    } */

                }
                if (distance<max) { // if any point is ready
                    point.setCluster(cluster);
                    Cluster aux = (Cluster) clusters.get(cluster);
                    aux.addPoint(point);
                }

            }

        }
        /*
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
        */
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

        ConcurrentMap points = Point.createRandomPoints(minCoordinate, maxCoordinate, num_points, instance);
        ConcurrentMap<Integer,Integer> clearIter = instance.getMap("clearIter");
        ConcurrentMap clusters = init(numClusters, minCoordinate, maxCoordinate, instance, clearIter);

        IAtomicLong count = instance.getAtomicLong("count");
        long localCount = count.incrementAndGet();
        if (localCount>numNodes){
            // Todo: create distributed long for numNodes and update it as needed
            System.out.println("number of nodes increased");
            return;
        }

        int pointsPart = points.size()/numNodes;
        int clustersPart = clusters.size()/numNodes;
        System.out.println(pointsPart+" "+clustersPart);

        calculate(clusters, points, clustersPart, pointsPart, localCount, numNodes, clearIter, instance);
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

        ConcurrentMap points = instance.getMap("points");
        ConcurrentMap clusters = instance.getMap("clusters");
        ConcurrentMap<Integer,Integer> clearIter = instance.getMap("clearIter");

        IAtomicLong count = instance.getAtomicLong("count");
        long localCount = count.incrementAndGet();
        if (localCount>numNodes){
            // Todo: create distributed long for numNodes and update it as needed
            System.out.println("number of nodes increased");
            return;
        }

        int pointsPart = points.size()/numNodes;
        int clustersPart = clusters.size()/numNodes;



        calculate(clusters, points, clustersPart, pointsPart, localCount, numNodes, clearIter, instance);

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



