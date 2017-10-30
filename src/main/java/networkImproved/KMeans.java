package networkImproved;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.map.AbstractEntryProcessor;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


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
    public static void calculate(String centroids, String clusterPoints, String points, int clustersPart, int pointsPart, long localCount, int numNodes, String clearIter, HazelcastInstance instance, String clusterSize) {
        boolean finish = false;
        long iteration = 1;
        double distance;


        List<Integer> localClustersSize = new ArrayList<>(instance.getMap(centroids).size());
        List<Point> localCentroids = new ArrayList<>(instance.getMap(centroids).size());
        Point initPoint = new Point();
        for (int i = 0; i < instance.getMap(centroids).size() ; i++) {
            localClustersSize.add(i,0);
            localCentroids.add(i,initPoint);
        }
        //getLocalCentroids(centroids, clustersPart, localCount, numNodes, localCentroids, instance, localClustersSize);   // fills localCentroids up

        instance.getAtomicLong("resetDone").set(0);


        while(!finish) {

            /*
            * The set-to-0 operations must be done only by one process, to ensure that, these operations
            * are only performed when "resetDone" is 0,
            * "resetDone" is only switched to 0 once per iteration
            * */

            instance.getAtomicLong("iterationFinished").incrementAndGet();
            while (instance.getAtomicLong("iterationFinished").get() != numNodes){
                // wait until all processes have finished adding their distance ("alter" func)
            }

            instance.getAtomicLong("resetDone").compareAndSet(1,0);

            instance.getLock("resetLock").lock();
                if (instance.getAtomicLong("resetDone").get() == 0) {
                    instance.getAtomicReference("distance").set(0.0);
                    instance.getAtomicLong("distanceCompleted").compareAndSet(numNodes, 0L);
                    instance.getAtomicLong("assignsFinished").compareAndSet(numNodes, 0L);  // reset assignsFinished for next iteration
                }
                instance.getAtomicLong("resetDone").set(1);
            instance.getLock("resetLock").unlock();



            // Assign points to the closest cluster
            assignCluster(centroids, clusterPoints, points, pointsPart, localCount, numNodes, iteration, clearIter, instance, localCentroids, localClustersSize);




            //int i = (int) ((localCount-1)*clustersPart);



            /*
            for (Point localCentroid: localCentroids ) {
                System.out.println("-------------------               J: "+j);
                currentCentroidGlobalX=localCentroid.getX();
                currentCentroidGlobalY=localCentroid.getY();

                System.out.println("before entry processor 1"+instance.getMap(centroids).get(j));
                System.out.println("about to add : "+currentCentroidGlobalX+", "+currentCentroidGlobalY);
                instance.getMap(centroids).executeOnKey(j,new AddCurrentCentroidEntryProcessor());
                System.out.println("after entry processor 1"+instance.getMap(centroids).get(j));

                if (localCount == 1){
                    System.out.println("waiting "+System.currentTimeMillis());
                    try {
                        TimeUnit.SECONDS.sleep(30L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("finish waiting "+System.currentTimeMillis());

                }

                System.out.println("before entry processor 2: "+instance.getMap(clusterSize).get(j));
                currentClusterSize=localClustersSize.get(j);
                System.out.println("about to add: "+currentClusterSize+" to "+instance.getMap(clusterSize).get(j));
                Object aa = instance.getMap(clusterSize).executeOnKey(j, new AddCurrentSizeEntryProcessor());
                /*
                while (currentClusterSize != -1){
                    System.out.println("        REPEATING "+currentClusterSize);
                    instance.getMap(clusterSize).executeOnKey(j, new AddCurrentSizeEntryProcessor());
                    if (localCount == 2){modifyCCC();}
                    System.out.println("        22REPEATING "+currentClusterSize);

                }
                *//*
                System.out.println("finishing this J with a clusterSize value : "+instance.getMap(clusterSize).get(j)+" ;  aa: "+aa.toString());


                //i++;
                j++;
            }
            */

            int j =0;

            System.out.println("localClustersSize before: "+localClustersSize);
            System.out.println("size: "+localClustersSize.size());

            instance.getMap("globalClusterSize").put((int)localCount-1, localClustersSize);
            System.out.println("globalClusterSize ("+(int)(localCount-1)+") after: "+instance.getMap("globalClusterSize").get((int)localCount-1));



            System.out.println("localCentroids before: "+localCentroids);
            System.out.println("size: "+localCentroids.size());

            instance.getMap("globalCentroids").put((int)localCount-1, localCentroids);
            System.out.println("globalCentroids ("+(int)(localCount-1)+") after: "+instance.getMap("globalCentroids").get((int)localCount-1));

            System.out.println("----------PROCESSING ENTRY FINISHED-----------");







            instance.getAtomicLong("assignsFinished").incrementAndGet();
            while (instance.getAtomicLong("assignsFinished").get() != numNodes) {
                // while "assignClusters" not finished in all processes don't start "calculateCentroids"

            }

            int acc;
            Point accPoint;
            if (localCount == 1){
                System.out.println("clusterSize before: "+instance.getMap(clusterSize).entrySet() );
                System.out.println("centroids before: "+instance.getMap(centroids).entrySet() );
                ArrayList<Integer> auxArray;
                Point auxPoint;
                for (int i = 0; i < instance.getMap(clusterSize).size() ; i++) {
                    acc=0;
                    accPoint=new Point();
                    for (int k = 0; k < instance.getMap("globalCentroids").size(); k++) {

                        System.out.println("k: "+k);
                        System.out.println("    globalClusterSize: "+instance.getMap("globalClusterSize").get(k));
                        auxArray= (ArrayList) instance.getMap("globalClusterSize").get(k);
                        acc += auxArray.get(i);

                        System.out.println("    globalCentroids: "+instance.getMap("globalCentroids").get(k));
                        auxPoint=(Point) ((ArrayList) instance.getMap("globalCentroids").get(k)).get(i);
                        accPoint.setX(accPoint.getX()+auxPoint.getX());
                        accPoint.setY(accPoint.getY()+auxPoint.getY());


                    }
                    instance.getMap(clusterSize).replace(i,  ((int) instance.getMap(clusterSize).get(i) ) + acc );

                    accPoint.setX(accPoint.getX()/(int)instance.getMap(clusterSize).get(i));
                    accPoint.setY(accPoint.getY()/(int)instance.getMap(clusterSize).get(i));
                    instance.getMap(centroids).replace(i, accPoint);

                }
                System.out.println("------ RESULTS: ------");
                System.out.println("clusterSize after: "+instance.getMap(clusterSize).entrySet() );
                System.out.println("centroids after: "+instance.getMap(centroids).entrySet() );




            } else {
                try {
                    TimeUnit.SECONDS.wait(30L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println("exiting");
            return; /*
            // As this call is between 2 waits for all processes is safe
            instance.getAtomicLong("iterationFinished").set(0);

            //Calculate new centroids.
            calculateCentroids(centroids, clusterPoints, clustersPart, localCount, numNodes, instance);

            // Calculates total distance between new and old Centroids
            distance = 0;
            int i = (int) ((localCount-1)*clustersPart);
            for (Point oldCentroid: localCentroids ) {
                Point currentCentroid = (Point) instance.getMap(centroids).get(i);
                distance += Point.distance(oldCentroid, currentCentroid);

                i++;
            }

            final double IterationDistance = distance;  // In order be used inside the overridden "apply", distance must be final

            // Add local copy to distributed variable "distance"
            instance.getAtomicReference("distance").alter(new IFunction<Object, Object>() {
                @Override
                public Object apply(Object o) {
                    return new Double(IterationDistance+ ((double) o));
                }

            });


            instance.getAtomicLong("distanceCompleted").incrementAndGet();
            while (instance.getAtomicLong("distanceCompleted").get() != numNodes){
                // wait until all processes have finished adding their distance ("alter" func)
            }


            if ( (double)instance.getAtomicReference("distance").get() < 0.01){
                System.out.println("Distance limit reached! distance: "+instance.getAtomicReference("distance").get() + " in iteration: "+iteration);
                finish=true;
            }

            iteration++;
            System.out.println("Iteration: "+iteration+" with a distance: "+instance.getAtomicReference("distance").get());
        */}

    }

    private static void clearClusters(String clusterPoints, int clustersPart, long localCount, int numNodes, long iteration, String clearIter, HazelcastInstance instance) {
        int module = 0;
        int clusterIter;

        if (instance.getMultiMap(clusterPoints).size() != 0) {    // first iteration won't have any points yet

            if (localCount == numNodes) { // if it's last node
                module = instance.getMap(clearIter).size() % numNodes;
            }

            for (int i = (int) ((localCount - 1) * clustersPart); i < ((localCount - 1) * clustersPart) + clustersPart + module; i++) {
                // walk through its part
                clusterIter = (int) instance.getMap(clearIter).get(i);
                if (clusterIter < iteration) {   // if cluster needs to be cleared
                    if (instance.getMultiMap(clusterPoints).size() > 0) {
                        instance.getMultiMap(clusterPoints).remove(i);
                    }
                    instance.getMap(clearIter).replace(i, (int) iteration);
                }
            }

        }

    }
    private static void getLocalCentroids(String centroids, int clustersPart, long localCount, int numNodes, List<Point> localCentroids, HazelcastInstance instance, List<Integer> localClustersSize){
        int module = 0;

        localCentroids.clear(); // avoids mixing centroids from different iterations

        if (localCount == numNodes) { // if it's last node
            module=instance.getMap(centroids).size()%numNodes;
        }

        for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart + module; i++) {
            // walk through its part

            Point point = new Point();
            Point aux = (Point) instance.getMap(centroids).get(i);
            point.setX(aux.getX());
            point.setY(aux.getY());
            localCentroids.add(point);

            localClustersSize.add(0);

        }
    }

    private static void assignCluster(String centroids, String clusterPoints, String points, int pointsPart, long localCount, int numNodes, long iteration, String clearIter, HazelcastInstance instance, List<Point> localCentroids, List<Integer> localClustersSize) {
        double max = Double.MAX_VALUE;
        double min = max;
        int cluster = 0;
        double distance = 0.0;
        int repetitionMax;
        final int REPETITION_LIMIT = 200;
        int module = 0;
        List<Integer> delays = new ArrayList<>();
        Point newCentroid;
        Point currentCentroid = new Point();


        if (localCount == numNodes) { // if it's last node
            module = instance.getMap(points).size() % numNodes;
        }

        for (int i = (int) ((localCount - 1) * pointsPart); i < (localCount - 1) * pointsPart + pointsPart + module; i++) {     // for each point
            // walk through its part
            min = max;

            for (int j = 0; j < instance.getMap(centroids).size(); j++) {     // assign to the closest cluster
                if ((int) instance.getMap(clearIter).get(j) == iteration || iteration==1 || instance.getMultiMap(clusterPoints).containsKey(j) == false ) {    // if cluster has been cleared (first iter doesn't clear)
                    distance = Point.distance((Point) instance.getMap(points).get(i), (Point) instance.getMap(centroids).get(j));

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
                    System.out.println(localCount+": WARNING: cluster " + j + " is taking too long to clear");
                    // Todo: decide what to do when cluster takes too long to clear
                    // Temporary debug:
                   /* debugEnd(localCount, false, j);
                    return 1;*/
                }


                if ((int) instance.getMap(clearIter).get(j) == iteration || iteration==1 || instance.getMultiMap(clusterPoints).containsKey(j) == false ) { // // if cluster has been cleared
                    repetitionMax = REPETITION_LIMIT;
                    distance = Point.distance((Point) instance.getMap(points).get(i), (Point) instance.getMap(centroids).get(j));
                    if (distance < min) {
                        min = distance;
                        cluster = j;
                    }
                } else {    // retry same cluster
                    j--;
                    repetitionMax--;
                }
            }
            if (distance < max) {   // if any point is ready
                newCentroid = new Point();
                //instance.getMap(points).get(i).setCluster(cluster);                          // mark point as ready for next stage (calculateCentroids)
                //clusterPoints.put(cluster, points.get(i));
                //instance.getMultiMap(clusterPoints).put(cluster, instance.getMap(points).get(i));
                currentCentroid = (Point) instance.getMap(points).get(i);
                newCentroid.setX(localCentroids.get(cluster).getX()+currentCentroid.getX() );
                newCentroid.setY(localCentroids.get(cluster).getY()+currentCentroid.getY() );
                localCentroids.set(cluster, newCentroid);
                System.out.println("    Adding newCentroid "+newCentroid.toString()+" to position: "+cluster);
                System.out.println("Adding 1 to J: "+cluster+" which had a value of: "+localClustersSize.get(cluster));
                localClustersSize.set(cluster, localClustersSize.get(cluster)+1);
                System.out.println("after adding in AssignCluster: to J: "+cluster+" which has a value of: "+localClustersSize.get(cluster));

            }

        }


    }

    private static void calculateCentroids(String centroids, String clusterPoints, int clustersPart, long localCount, int numNodes, HazelcastInstance instance) {
        int module = 0;
        double sumX;
        double sumY;

        double newX;
        double newY;

        int n_points;

        if (localCount == numNodes) { // if it's last node
            module=instance.getMap(centroids).size()%numNodes;
        }

        for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart + module; i++) {      // for each cluster
            // walk through its part

            sumX=0;     // reset for each cluster
            sumY=0;
            MultiMap<Integer, Point> pointsMap =  instance.getMultiMap(clusterPoints);
            for (Point point: pointsMap.get(i) ) {      // for each of its points
                sumX += point.getX();                           // add to process local variables
                sumY += point.getY();                           // Todo: either use BigDecimal or check Double.POSITIVE_INFINITY or Double.NEGATIVE_INFINITY
            }

            Point centroid = (Point) instance.getMap(centroids).get(i);
            n_points = instance.getMultiMap(clusterPoints).get(i).size();
            if(n_points > 0) {
                newX = sumX / n_points;                  // compute avg
                newY = sumY / n_points;

                centroid.setX(newX);                            // set clusters avg
                centroid.setY(newY);
                instance.getMap(centroids).replace(i,centroid);
            }
        }

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

        calculate(centroids, clusterPoints, points, clustersPart, pointsPart, localCount, numNodes, clearIter, instance, clusterSize); // main call

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

        calculate(centroids, clusterPoints, points, clustersPart, pointsPart, localCount, numNodes, clearIter, client, clusterSize);

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

    private static class AddCurrentCentroidEntryProcessor extends AbstractEntryProcessor<Integer, Point> {
        @Override
        public Object process(Map.Entry<Integer, Point> entry) {
            Point point = entry.getValue();
            point.setX(point.getX()+currentCentroidGlobalX);
            point.setY(point.getY()+currentCentroidGlobalY);
            entry.setValue(point);

            return null;
        }
    }

    private static class AddCurrentSizeEntryProcessor extends AbstractEntryProcessor<Integer, Integer>  implements Offloadable {
        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            System.out.println("    inside func, before 'getValue()' ");
            int size = entry.getValue();
            System.out.println("    getKey: "+entry.getKey()+" ; getValue: "+entry.getValue());
            size=size+currentClusterSize;

            System.out.println("    inside func, just added :"+currentClusterSize);
            currentClusterSize=-1;
            entry.setValue(size);
            //entry.
            return entry;
        }

        @Override
        public String getExecutorName() {
            return "";
        }
    }
    private static void modifyCCC(){
        currentClusterSize=-5;
    }
}





