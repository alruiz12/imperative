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

    public KMeans() {}


    // Initializes the process
    public static ConcurrentMap<Integer, Cluster> init(int numClusters, int minCoordinate, int maxCoordinate, HazelcastInstance instance, Map<Integer, Integer> clearIter) {
        // Create Clusters
        ConcurrentMap<Integer, Cluster> clusters = instance.getMap("clusters");
        for (int i = 0; i < numClusters; i++) {
            // Create and initialize new cluster
            Cluster cluster = new Cluster();
            cluster.setID(i);

            // Set Random Centroids
            Point centroid = Point.createRandomPoint(minCoordinate,maxCoordinate);
            cluster.setCentroid(centroid);
            clusters.put(i, cluster);

            // Fills up clearIter entry
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

            //Assign points to the closest cluster
            assignCluster(clusters, points, pointsPart, localCount, numNodes, iteration, clearIter);


            System.out.println("calculate end (localcount: "+localCount+" )");

            //Calculate new centroids.
            //calculateCentroids(clusters, clustersPart, localCount, numNodes, iteration, clearIter, instance); // Todo now!!!!!!!!!!!!
/*

            List<Point> currentCentroids = getCentroids(clusters);

            //Calculates total distance between new and old Centroids
            AtomicDouble distance = new AtomicDouble();
            for(int i = 0; i < lastCentroids.size(); i++) {
                distance.getAndAdd(Point.distance(lastCentroids.get(i),currentCentroids.get(i)));
            }

            if (distance.compareAndSet(0.0,0.0)){
                finish = true;
            }
            */
            iteration++;
            finish=true;
        }

    }

    private static void clearClusters(Map clusters, int clustersPart, long localCount, int numNodes, long iteration, ConcurrentMap<Integer, Integer> clearIter, HazelcastInstance instance) {
        int module = 0;
        int clusterIter=0;

        if (localCount == numNodes) { // if it's last node
            module=clusters.size()%numNodes;
        }

        for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart + module; i++) {
            // walk through its part
            Cluster c = (Cluster) clusters.get(i);
            clusterIter = clearIter.get(i);
            if (clusterIter < iteration){   // if cluster needs to be cleared
                c.clear();
                clearIter.replace(i, clusterIter+1) ;
            }
        }

    }

    private static void assignCluster(Map clusters, Map points, int pointsPart, long localCount, int numNodes, long iteration, Map<Integer, Integer> clearIter) {
        double max = Double.MAX_VALUE;
        double min = max;
        int cluster = 0;
        double distance = 0.0;
        int repetitionMax;
        final int REPETITION_LIMIT = 10;
        int module = 0;
        List<Integer> delays = new ArrayList<>();

        if (localCount == numNodes) { // if it's last node
            module = points.size() % numNodes;
        }
        for (int i = (int) ((localCount - 1) * pointsPart); i < (localCount - 1) * pointsPart + pointsPart + module; i++) {
            // walk through its part
            Point point = (Point) points.get(i);

            for (int j = 0; j < clusters.size(); j++) {
                Cluster c = (Cluster) clusters.get(j);

                if (clearIter.get(j) == iteration) {    // if cluster has been cleared
                    System.out.println(clearIter.get(j) + " == " + iteration);
                    distance = Point.distance(point, c.getCentroid());
                    if (distance < min) {
                        min = distance;
                        cluster = j;
                    }
                } else {    // add to delayed list, and rerun
                    delays.add(j);
                }
            }

            repetitionMax = REPETITION_LIMIT;

            for (int j = 0; j < delays.size(); j++) {
                if (repetitionMax <= 0) {
                    System.out.println("WARNING: cluster " + j + " is taking too long to clear");
                    // Todo: decide what to do when cluster takes too long to clear
                }

                Cluster c = (Cluster) clusters.get(j);

                if (clearIter.get(j) == iteration) { // // if cluster has been cleared
                    repetitionMax = REPETITION_LIMIT;
                    distance = Point.distance(point, c.getCentroid());
                    if (distance < min) {
                        min = distance;
                        cluster = j;
                    }
                } else {    // retry same cluster
                    j--;
                    repetitionMax--;
                }
            }
            if (distance < max) { // if any point is ready
                point.setCluster(cluster);
                Cluster aux = (Cluster) clusters.get(cluster);
                aux.addPoint(point);
            }

        }


    }

    private static void calculateCentroids(ConcurrentMap clusters, int clustersPart, long localCount, int numNodes, long iteration, ConcurrentMap<Integer, Integer> clearIter, HazelcastInstance instance) {

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

        //----------------------------------------------------------------
        /*
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
            */
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

    public static void run(int numClusters, int num_points, int minCoordinate, int maxCoordinate, int numIter, int numNodes) {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();

        ConcurrentMap points = Point.createRandomPoints(minCoordinate, maxCoordinate, num_points, instance);
        ConcurrentMap<Integer,Integer> clearIter = instance.getMap("clearIter");        // Keeps track of the number of "clear" iterations of each cluster
        ConcurrentMap clusters = init(numClusters, minCoordinate, maxCoordinate, instance, clearIter);

        IAtomicLong count = instance.getAtomicLong("count");
        long localCount = count.incrementAndGet();      // As new processes run, they increment a counter and keep the local copy as their ID
        if (localCount>numNodes){
            // Todo: create distributed long for numNodes and update it as needed
            System.out.println("number of nodes increased");
            return;
        }

        int pointsPart = points.size()/numNodes;
        int clustersPart = clusters.size()/numNodes;

        calculate(clusters, points, clustersPart, pointsPart, localCount, numNodes, clearIter, instance);

        //end(clusters);

    }

    public static void runSecondary(int numClusters, int num_points, int minCoordinate, int maxCoordinate, int numIter, int numNodes) {
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
	instance.shutdown();
        //end(clusters);


    }

    }



