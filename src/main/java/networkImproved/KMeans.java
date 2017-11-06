package networkImproved;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;


/**
 * Created by alvaro on 27/09/17.
 */
public class KMeans {

    // Initializes the process
    public static void init(int numClusters, int minCoordinate, int maxCoordinate, String centroids, HazelcastInstance instance, String clusterSize) {

        for (int i = 0; i < numClusters; i++) {

            // Set Random Centroids
            instance.getMap(centroids).put(i, createRandomPoint(minCoordinate,maxCoordinate));

            // Set all the cluster's size to 0
            instance.getMap(clusterSize).put(i,0);

            // Initializes global data structures
            instance.getMap("glo1balCentroids ").put(i, new ArrayList<Integer>());
            instance.getMap("globalClusterSize").put(i, new ArrayList<Integer>());

        }
    }

    // The process to calculate the K Means, with iterating method.
    public static void calculate(String centroids, String points,  int pointsPart, long localCount, int numNodes, HazelcastInstance instance, String clusterSize, int[] membership) {
        boolean finish = false;
        String fileName;
        double delta, deltaTmp = 0.0;
        List<Integer> localClustersSize = new ArrayList<>(instance.getMap(centroids).size());
        List<double[]> localCentroids = new ArrayList<>(instance.getMap(centroids).size());
        HashMap localPoints = new HashMap<Integer, double[]>();
        int module=0;
        int nlines=0;
        String line;

        if (localCount == numNodes) { // if it's last node
            module = membership.length % numNodes;
        }
        if (!loadDataset(localCount, pointsPart, localPoints, module))return;
        System.out.println("load ok ");

        double[] emptyPoint = {0,0};    // Todo: make it n dimension

        for (int i = 0; i < instance.getMap(centroids).size() ; i++) {
            // Initialize local data structures
            localClustersSize.add(i,0);
            localCentroids.add(i,emptyPoint);
        }

        while(!finish) {

            // Assign points to the closest cluster
            delta = assignCluster(centroids, points, pointsPart, localCount, numNodes, instance, localCentroids, localClustersSize, membership, localPoints);

            // Load local data structures to the global ones
            instance.getMap("globalClusterSize").put((int)localCount-1, localClustersSize);
            instance.getMap("globalCentroids").put((int)localCount-1, localCentroids);

            // Empty the list of deltas
            instance.getList("deltaList").clear();

            // ------------------------------------------- BARRIER START -----------------------------------------------
            instance.getAtomicLong("assignsFinished").incrementAndGet();
            while (instance.getAtomicLong("assignsFinished").get() != numNodes) {
                // waiting for all processes to assign clusters and update global data structures
            }
            // ------------------------------------------- BARRIER END -------------------------------------------------

            // Will be set to true when all processes are ready to move on to the next iteration
            instance.getAtomicLong("iterationFinished").set(0L);

            // Add local delta to the global data structure
            instance.getList("deltaList").add(delta);

            if (localCount == 1){   // Only one process will reduce the global data structures (In this case process 1)
                int acc;
                double[] accPoint;

                for (int i = 0; i < instance.getMap(clusterSize).size() ; i++) {
                    acc=0;
                    accPoint=new double[2];
                    for (int k = 0; k < instance.getMap("globalCentroids").size(); k++) {

                        // Accumulate all data from global data structures

                        acc += (int)((ArrayList) instance.getMap("globalClusterSize").get(k) ).get(i);

                        accPoint[0] += ((double[]) ((ArrayList) instance.getMap("globalCentroids").get(k)).get(i))[0];
                        accPoint[1] += ((double[]) ((ArrayList) instance.getMap("globalCentroids").get(k)).get(i))[1];

                    }
                    if (acc>0) {    // if cluster has points
                        instance.getMap(clusterSize).replace(i,  ((int) instance.getMap(clusterSize).get(i) ) + acc );
                    }
                    accPoint[0] = accPoint[0] / (int)instance.getMap(clusterSize).get(i);
                    accPoint[1] = accPoint[1] / (int)instance.getMap(clusterSize).get(i);

                    instance.getMap(centroids).replace(i, accPoint);

                    // Reset local data structures (otherwise they will be added to themselves again)
                    localCentroids.set(i,emptyPoint);
                    localClustersSize.set(i,0);
                }

                deltaTmp=0.0;
                for (int m = 0; m < instance.getList("deltaList").size() ; m++) {
                    deltaTmp += (double) instance.getList("deltaList").get(m);
                    instance.getList("deltaList").remove(m);
                }
                delta=deltaTmp/membership.length;   // num of modifications / num points

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
                while (instance.getAtomicLong("iterationFinished").get() == 0L){

                }
                if (instance.getAtomicLong("iterationFinished").get() == -1L ){
                    finish = true;
                }
            }

        }
    }


    private static double assignCluster(String centroids, String points, int pointsPart, long localCount, int numNodes, HazelcastInstance instance, List<double[]> localCentroids, List<Integer> localClustersSize, int[] membership, HashMap<Integer, double[]> localPoints) {
        double max = Double.MAX_VALUE;
        double min = max;
        int cluster = 0;
        double distance = 0.0;
        int module = 0;
        double delta = 0.0;
        double[] currentCentroid={0,0};

        if (localCount == numNodes) { // if it's last node
            module = instance.getMap(points).size() % numNodes;
        }

        for (int i = (int) ((localCount - 1) * pointsPart); i < (localCount - 1) * pointsPart + pointsPart + module; i++) {     // for each point
            // walk through its part
            min = max;

            for (int j = 0; j < instance.getMap(centroids).size(); j++) {     // assign to the closest cluster

                distance = distance( localPoints.get(i), (double[]) instance.getMap(centroids).get(j));
                if (distance < min) {
                    min = distance;
                    cluster = j;
                }
            }

            if (distance < max) {   // if any point is ready

                currentCentroid = localPoints.get(i);
                double[] newCentroid = {(localCentroids.get(cluster)[0]+currentCentroid[0]),localCentroids.get(cluster)[1]+currentCentroid[1] };

                localCentroids.set(cluster, newCentroid);
                localClustersSize.set(cluster, localClustersSize.get(cluster)+1);

                if (membership[i] != cluster) delta += 1.0;

                membership[i] = cluster;

            }

        }
        return delta;
    }

    public static void run(int numClusters, int num_points, int minCoordinate, int maxCoordinate,  int numNodes) {
        long startTime = System.currentTimeMillis();
        Config conf = new Config();
        conf.getGroupConfig().setName("kmeansName").setPassword("kmeansPass");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(conf);

        IAtomicLong finished = instance.getAtomicLong("finished");
        finished.set(0);
        String points = "points";
        createRandomPoints(minCoordinate, maxCoordinate, num_points, instance, points);

        String centroids = "centroids";
        String clusterSize="clusterSize";

        init(numClusters, minCoordinate, maxCoordinate, centroids, instance, clusterSize);      // Sets random centroids

        IAtomicLong count = instance.getAtomicLong("count");
        long localCount = count.incrementAndGet();      // As new processes run, they increment a counter and keep the local copy as their ID
        if (localCount>numNodes){
            // Todo: create distributed long for numNodes and update it as needed
            System.out.println("number of nodes increased");
            return;
        }
        System.out.println("+++++++++++++++++++++++++++++++++++ "+instance.getMap(points).size());
        int pointsPart = instance.getMap(points).size()/numNodes;
        int[] membership = new int[num_points];

        calculate(centroids, points, pointsPart, localCount, numNodes, instance, clusterSize, membership); // main call

        finished.incrementAndGet(); // Counts finished processes
        while (finished.get() != numNodes){
            // Waits for all processes to finish before obtaining elapsed time
        }

        long finalTime = System.currentTimeMillis();            // When all process finish, time elapsed time
        debugEnd((finalTime-startTime)/1000, true, -1);    // Create a file with info about time (avoids busy st out)
        debugEnd(localCount, true, 0);
        instance.shutdown();

    }

    public static void runSecondary(int numClusters, int num_points, int numNodes) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(15);
        clientConfig.getGroupConfig().setName("kmeansName").setPassword("kmeansPass");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        String points = "points";
        String centroids = "centroids";
        String clusterSize="clusterSize";

        while(client.getMap(points).size()!=num_points || client.getMap(centroids).size() != numClusters ){
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
        int[] membership = new int[num_points];

        calculate(centroids, points, pointsPart, localCount, numNodes, client, clusterSize, membership); // main call

        IAtomicLong finished = client.getAtomicLong("finished");
        finished.incrementAndGet();

        debugEnd(localCount, true, 0);

        client.shutdown();

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



    private static double[] createRandomPoint(int min, int max){
        Random r = new Random();
        double[] aux = {min + (max - min) * r.nextDouble(), min + (max - min) * r.nextDouble()};
        return aux;
    }

    private static void createRandomPoints(int min, int max, int number, HazelcastInstance instance, String points){
        ConcurrentMap<Integer, double[]> pointsMap = instance.getMap(points);
        for(int i = 0; i<number; i++) {
            pointsMap.put(i,createRandomPoint(min,max));
        }
    }


    //Calculates the distance between two points.
    protected static double distance(double[] p, double[] centroid) {
        return Math.sqrt(Math.pow((centroid[1] - p[1]), 2) + Math.pow((centroid[0] - p[0]), 2));
    }

    private static boolean loadDataset(long localCount, int pointsPart, HashMap localPoints, int module){
        String fileName, line;
        int nlines=0;
        double[] pointLine;

        if (localCount-1 < 10) {
            fileName = "/home/alvaro/imperative/input/x0"+(localCount-1);
        } else{
            fileName = "/home/alvaro/imperative/input/x"+(localCount-1);
        }
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            for (int i = (int) ((localCount - 1) * pointsPart); i < (localCount - 1) * pointsPart + pointsPart + module; i++) {
                line= bufferedReader.readLine();

                if (line==null){
                    if (nlines < (pointsPart+module)){                                  // still lines to be read
                        if (localCount-1 < 10) {
                            fileName = "/home/alvaro/imperative/input/x0"+(localCount); // leftover file
                        } else{
                            fileName = "/home/alvaro/imperative/input/x"+(localCount);
                        }
                        bufferedReader = new BufferedReader(new FileReader(fileName));
                        System.out.println("line is null OK");
                        System.out.println("    fileName: "+fileName);
                        System.out.println("    i: "+i);
                        System.out.println("    nlines: "+nlines+" ;  pointsPart + module: "+ (pointsPart + module));
                        System.out.println("    module: "+module);
                        i--; // retry iteration
                    } else{
                        System.out.println("ERROR: line is null");
                        System.out.println("fileName: "+fileName);
                        System.out.println("i: "+i);
                        System.out.println("nlines: "+nlines+" ;  pointsPart + module: "+ (pointsPart + module));
                        System.out.println("module: "+module);
                        return false;
                    }
                } else {
                    pointLine = Arrays.asList(line.split(",")).stream().mapToDouble(Double::parseDouble).toArray();
                    localPoints.put(i, pointLine);
                    nlines++;
                }
            }

            System.out.println("nlines: "+nlines+" ;  pointsPart + module: "+ (pointsPart + module));
            System.out.println("module: "+module);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}





