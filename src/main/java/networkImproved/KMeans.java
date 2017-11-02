package networkImproved;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * Created by alvaro on 27/09/17.
 */
public class KMeans {

    //public KMeans() {}

    static double currentCentroidGlobalX = 0.0;
    static double currentCentroidGlobalY = 0.0;
    static int currentClusterSize = 0;


    // Initializes the process
    public static void init(int numClusters, int minCoordinate, int maxCoordinate, String centroids, String clearIter, HazelcastInstance instance, String clusterSize) {

        /*
        * As the elements of a plain Java List inside a Java Object cannot be modified concurrently
        * and the List cannot be implemented as a Hazelcast List (inside of an Object),
        * the Object Cluster is broken into 2 Hazelcast data structures referenced by the same key
        * ( ConcurrentMap for the field "centroid" (a Point) and MultiMap for the field "points" (a List of Points) )
        * */


        for (int i = 0; i < numClusters; i++) {

            // Set Random Centroids
            Point centroid = Point.createRandomPoint(minCoordinate,maxCoordinate);
            instance.getMap(centroids).put(i, centroid);

            // Fill up clearIter entry
            instance.getMap(clearIter).put(i,0);

            instance.getMap(clusterSize).put(i,0);

            instance.getMap("globalCentroids ").put(i, new ArrayList<Integer>());
            instance.getMap("globalClusterSize").put(i, new ArrayList<Integer>());

        }

    }

    // The process to calculate the K Means, with iterating method.
    public static void calculate(String centroids, String clusterPoints, String points, int clustersPart, int pointsPart, long localCount, int numNodes, String clearIter, HazelcastInstance instance, String clusterSize, int[] membership) {
        boolean finish = false;
        long iteration = 1;
        double distance;
        Point emptyPoint = new Point();

        List<Integer> localClustersSize = new ArrayList<>(instance.getMap(centroids).size());
        List<Point> localCentroids = new ArrayList<>(instance.getMap(centroids).size());
        Point initPoint = new Point();

        HashMap localPoints = new HashMap<Integer, Point>();

        int module=0;
        if (localCount == numNodes) { // if it's last node
            module = membership.length % numNodes;
        }
        for (int i = (int) ((localCount - 1) * pointsPart); i < (localCount - 1) * pointsPart + pointsPart + module; i++) {     // for each point
            localPoints.put(i,instance.getMap(points).get(i));
        }

        for (int i = 0; i < instance.getMap(centroids).size() ; i++) {
            localClustersSize.add(i,0);
            localCentroids.add(i,initPoint);
        }

        instance.getAtomicLong("resetDone").set(0);

        double delta, deltaTmp = 0.0;

        while(!finish) {


            // Assign points to the closest cluster
            delta = assignCluster(centroids, clusterPoints, points, pointsPart, localCount, numNodes, iteration, clearIter, instance, localCentroids, localClustersSize, membership, localPoints);

/*
            System.out.println("localClustersSize before: "+localClustersSize);
            System.out.println("size: "+localClustersSize.size());
*/
            instance.getMap("globalClusterSize").put((int)localCount-1, localClustersSize);
/*            System.out.println("globalClusterSize ("+(int)(localCount-1)+") after adding localClusterSize: "+instance.getMap("globalClusterSize").get((int)localCount-1));

            System.out.println("-----------------------------");

            System.out.println("localCentroids before: "+localCentroids);
            System.out.println("size: "+localCentroids.size());
*/
            instance.getMap("globalCentroids").put((int)localCount-1, localCentroids);
/*            System.out.println("globalCentroids ("+(int)(localCount-1)+") after adding localCentroids: "+instance.getMap("globalCentroids").get((int)localCount-1));

            System.out.println("----------GLOBAL DATA STRUCTURES UPDATED-----------");
*/




            // ------------------------------------------- BARRIER START -----------------------------------------------

            instance.getList("deltaList").clear();
//           System.out.println( "assignsFinished: "+instance.getAtomicLong("assignsFinished").get()+" TIME: "+System.currentTimeMillis());
            instance.getAtomicLong("assignsFinished").incrementAndGet();
            while (instance.getAtomicLong("assignsFinished").get() != numNodes) {
                // while "assignClusters" not finished in all processes don't start "calculateCentroids"
            }
//            System.out.println( "assignsFinished after: "+instance.getAtomicLong("assignsFinished").get()+" TIME: "+System.currentTimeMillis());

            // ------------------------------------------- BARRIER END -------------------------------------------------

            instance.getAtomicLong("iterationFinished").set(0L);


            instance.getList("deltaList").add(delta); //.add((int )localCount-1, delta);
        //    System.out.println("AFTER adding, delta: "+instance.getList("deltaList").get((int )localCount-1)+" ; size: "+instance.getList("deltaList").size());
//            System.out.println("deltaList start: ");
/*
            for (int i = 0; i < instance.getList("deltaList").size(); i++) {
                System.out.println("    "+instance.getList("deltaList").get(i).toString());
            }
            System.out.println("deltaList end");
*/

            if (localCount == 1){
                int acc;
                Point accPoint;
/*                System.out.println("clusterSize before: "+instance.getMap(clusterSize).entrySet() );
                System.out.println("centroids before: "+instance.getMap(centroids).entrySet() );*/
                //ArrayList<Integer> auxArray;
                Point auxPoint;
                for (int i = 0; i < instance.getMap(clusterSize).size() ; i++) {
                    acc=0;
                    accPoint=new Point();
                    for (int k = 0; k < instance.getMap("globalCentroids").size(); k++) {

/*                        System.out.println("k: "+k);
                        System.out.println("    globalClusterSize: "+instance.getMap("globalClusterSize").get(k));  */
                        acc += (int)((ArrayList) instance.getMap("globalClusterSize").get(k) ).get(i);
                        //acc += auxArray.get(i);

//                        System.out.println("    globalCentroids: "+instance.getMap("globalCentroids").get(k));
                        auxPoint=(Point) ((ArrayList) instance.getMap("globalCentroids").get(k)).get(i);
                        accPoint.setX(accPoint.getX()+auxPoint.getX());
                        accPoint.setY(accPoint.getY()+auxPoint.getY());

                    }
                    if (acc>0) {    // if cluster has points
                        instance.getMap(clusterSize).replace(i,  ((int) instance.getMap(clusterSize).get(i) ) + acc );
                    }

                    accPoint.setX(accPoint.getX()/(int)instance.getMap(clusterSize).get(i));
                    accPoint.setY(accPoint.getY()/(int)instance.getMap(clusterSize).get(i));
                    instance.getMap(centroids).replace(i, accPoint);

                    // Reset local data structures (otherwise they will be added to themselves again)
                    localCentroids.set(i,emptyPoint);
                    localClustersSize.set(i,0);
                }
/*                System.out.println("------ RESULTS: ------");
                System.out.println("clusterSize after: "+instance.getMap(clusterSize).entrySet() );
                System.out.println("centroids after: "+instance.getMap(centroids).entrySet() );*/
                deltaTmp=0.0;
                for (int m = 0; m < instance.getList("deltaList").size() ; m++) {
                    deltaTmp += (double) instance.getList("deltaList").get(m);
                    instance.getList("deltaList").remove(m);
                }
                delta=deltaTmp/membership.length;
    //            System.out.println("deltaTmp: "+deltaTmp);
    //            System.out.println("membership.length: "+membership.length);
                System.out.println("********** DELTA UPDATED: "+delta+" ***************");
                if (delta<0.001) {
                    finish = true;
                    instance.getAtomicLong("iterationFinished").set(-1L);

                } else {
                    instance.getAtomicLong("assignsFinished").compareAndSet(numNodes, 0L);  // reset assignsFinished for next iteration
                    instance.getAtomicLong("iterationFinished").set(1L);
                }

            } else {
                for (int i = 0; i < instance.getMap(clusterSize).size() ; i++) {
                    // Reset local data structures (otherwise they will be added to themselves again)
                    localCentroids.set(i,emptyPoint);
                    localClustersSize.set(i,0);
                }
                //System.out.println("8   8   before while iterationFinished == 0");
                while (instance.getAtomicLong("iterationFinished").get() == 0L){

                }
                //System.out.println("8   8   iterationFinished != 0");
                if (instance.getAtomicLong("iterationFinished").get() == -1L ){
                    finish = true;
                }
            }

            }

    }


    private static double assignCluster(String centroids, String clusterPoints, String points, int pointsPart, long localCount, int numNodes, long iteration, String clearIter, HazelcastInstance instance, List<Point> localCentroids, List<Integer> localClustersSize, int[] membership, HashMap<Integer, Point> localPoints) {
        double max = Double.MAX_VALUE;
        double min = max;
        int cluster = 0;
        double distance = 0.0;
        int repetitionMax;
        final int REPETITION_LIMIT = 200;
        int module = 0;
        double delta = 0.0;
        List<Integer> delays = new ArrayList<>();
        Point newCentroid;
        Point currentCentroid = new Point();
        //System.out.println("INSIDE ASSIGN CLUSTER");

        if (localCount == numNodes) { // if it's last node
            module = instance.getMap(points).size() % numNodes;
        }

        for (int i = (int) ((localCount - 1) * pointsPart); i < (localCount - 1) * pointsPart + pointsPart + module; i++) {     // for each point
            // walk through its part
            min = max;

            for (int j = 0; j < instance.getMap(centroids).size(); j++) {     // assign to the closest cluster

                distance = Point.distance( localPoints.get(i), (Point) instance.getMap(centroids).get(j));

                if (distance < min) {
                    min = distance;
                    cluster = j;
                }

            }


            if (distance < max) {   // if any point is ready
                newCentroid = new Point();
                //instance.getMap(points).get(i).setCluster(cluster);                          // mark point as ready for next stage (calculateCentroids)
                //clusterPoints.put(cluster, points.get(i));
                //instance.getMultiMap(clusterPoints).put(cluster, instance.getMap(points).get(i));
                currentCentroid = localPoints.get(i);
                newCentroid.setX(localCentroids.get(cluster).getX()+currentCentroid.getX() );
                newCentroid.setY(localCentroids.get(cluster).getY()+currentCentroid.getY() );
                localCentroids.set(cluster, newCentroid);
/*                System.out.println("    Adding newCentroid "+newCentroid.toString()+" to position: "+cluster);
                System.out.println("    Adding 1 to J: "+cluster+" which had a value of: "+localClustersSize.get(cluster));*/
                localClustersSize.set(cluster, localClustersSize.get(cluster)+1);
         //       System.out.println("    after adding in AssignCluster: to J: "+cluster+" which has a value of: "+localClustersSize.get(cluster));

                if (membership[i] != cluster) delta += 1.0;

                membership[i] = cluster;

            }

        }
 //       System.out.println("ASSIGN CLUSTER: RETURNING BIG DELTA OF "+delta);
        return delta;
    }

    public static void run(int numClusters, int num_points, int minCoordinate, int maxCoordinate, int numIter, int numNodes) {
        long startTime = System.currentTimeMillis();
        Config conf = new Config();
        conf.getGroupConfig().setName("kmeansName").setPassword("kmeansPass");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(conf);

        IAtomicLong finished = instance.getAtomicLong("finished");
        finished.set(0);
        String points = "points";
        Point.createRandomPoints(minCoordinate, maxCoordinate, num_points, instance, points);

        String clearIter = "clearIter";        // Keeps track of the number of "clear" iterations of each cluster
        String centroids = "centroids";
        String clusterPoints = "clusterPoints";
        String clusterSize="clusterSize";

        init(numClusters, minCoordinate, maxCoordinate, centroids, clearIter, instance, clusterSize);      // Sets random centroids and initializes clearIter

        IAtomicLong count = instance.getAtomicLong("count");
        long localCount = count.incrementAndGet();      // As new processes run, they increment a counter and keep the local copy as their ID
        if (localCount>numNodes){
            // Todo: create distributed long for numNodes and update it as needed
            System.out.println("number of nodes increased");
            return;
        }

        int pointsPart = instance.getMap(points).size()/numNodes;
        int clustersPart = instance.getMap(centroids).size()/numNodes;
        int[] membership = new int[num_points];

        calculate(centroids, clusterPoints, points, clustersPart, pointsPart, localCount, numNodes, clearIter, instance, clusterSize, membership); // main call

        finished.incrementAndGet(); // Counts finished processes
        while (finished.get() != numNodes){
            // Waits for all processes to finish before obtaining elapsed time
        }


        long finalTime = System.currentTimeMillis();            // When all process finish, time elapsed time
        debugEnd((finalTime-startTime)/1000, true, -1);    // Create a file with info about time (avoids busy st out)
        debugEnd(localCount, true, 0);
        //end(clusters);
        instance.shutdown();

    }

    public static void runSecondary(int numClusters, int num_points, int minCoordinate, int maxCoordinate, int numIter, int numNodes) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(15);
        clientConfig.getGroupConfig().setName("kmeansName").setPassword("kmeansPass");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        String points = "points";
        String clearIter = "clearIter";        // Keeps track of the number of "clear" iterations of each cluster
        String centroids = "centroids";
        String clusterPoints = "clusterPoints";
        String clusterSize="clusterSize";

        while(client.getMap(points).size()!=num_points || client.getMap(centroids).size() != numClusters || client.getMap(clearIter).size() != numClusters){
            // Wait for initialization
            // Todo: could be optimized putting thread to sleep, now hazelcast exception stops execution
        }

        IAtomicLong count = client.getAtomicLong("count");
        long localCount = count.incrementAndGet();
        if (localCount>numNodes){
            // Todo: create distributed long for numNodes and update it as needed (thus avoids hard code)
            System.out.println("number of nodes increased, localcount: "+ localCount+", numNodes: "+numNodes);
            return;
        }

        int pointsPart = client.getMap(points).size()/numNodes;
        int clustersPart = client.getMap(centroids).size()/numNodes;
        int[] membership = new int[num_points];

        calculate(centroids, clusterPoints, points, clustersPart, pointsPart, localCount, numNodes, clearIter, client, clusterSize, membership); // main call

        IAtomicLong finished = client.getAtomicLong("finished");
        finished.incrementAndGet();

        debugEnd(localCount, true, 0);

        client.shutdown();
        //end(clusters);


    }

    // debugEnd debugs the execution of a process without using the (heavily used by Hazelcast) standard output
    public static void debugEnd(long localCount, boolean endSuccessful, int stoppedAt) {
        String pid = "_";
        if (stoppedAt == -1) {
            pid = "TIME=" + String.valueOf(localCount);
        } else {
            if (endSuccessful) {
                pid = String.valueOf(localCount) + "_OK";
            } else {
                pid = String.valueOf(localCount) + "KO!" + String.valueOf(stoppedAt);
            }
        }
            File file = new File(pid);
            try {
                PrintWriter printWriter = new PrintWriter(file);
                printWriter.print(pid);
                printWriter.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

    }

}





