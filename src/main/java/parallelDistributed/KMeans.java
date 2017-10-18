package parallelDistributed;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;

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
    public static void init(int numClusters, int minCoordinate, int maxCoordinate, ConcurrentMap<Integer, Point> centroids, Map<Integer, Integer> clearIter) {
        // Create Clusters

        /*
        * As the elements of a plain Java List inside a Java Object cannot be modified concurrently
        * and the List cannot be implemented as a Hazelcast List (inside of an Object),
        * the Object Cluster is broken into 2 Hazelcast data structures referenced by the same key
        * ( ConcurrentMap for the field "centroid" (a Point) and MultiMap for the field "points" (a List of Points) )
        * */



       // ConcurrentMap<Integer, Cluster> clusters = instance.getMap("clusters");
        for (int i = 0; i < numClusters; i++) {

            /*
            // Create and initialize new cluster
            Cluster cluster = new Cluster();
            cluster.setID(i);
            System.out.println("init 0");
            //cluster.setPoints(instance);
            System.out.println("init 1");
            */

            // Set Random Centroids
            Point centroid = Point.createRandomPoint(minCoordinate,maxCoordinate);
            centroids.put(i, centroid);

            /*
            cluster.setCentroid(centroid);
            clusters.put(i, cluster);
            */

            // Fills up clearIter entry
            clearIter.put(i,0);
        }


    }

    // The process to calculate the K Means, with iterating method.
    public static int calculate(ConcurrentMap<Integer, Point> centroids,  MultiMap<Integer, Point> clusterPoints, ConcurrentMap<Integer, Point> points, int clustersPart, int pointsPart, long localCount, int numNodes, ConcurrentMap<Integer, Integer> clearIter, HazelcastInstance instance) {
        boolean finish = false;
        long iteration = 1;
        int retValue=-1;
        List<Point> lastCentroids = new ArrayList<>();
        instance.getAtomicLong("resetDone").set(0);

        int module=0;
        if (localCount == numNodes) { // if it's last node
            module=centroids.size()%numNodes;
        }

        for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart + module; i++) {
            System.out.println("preCentroids: "+centroids.get(i));
        }

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



            // Clear clusters point list (doesn't clear centroids)
            clearClusters(clusterPoints, clustersPart, localCount, numNodes, iteration, clearIter, instance);

            // A copy of current centroids is saved in lastCentroids before they are recalculated
            getLocalCentroids(centroids, clustersPart, localCount, numNodes, lastCentroids);   // fills lastCentroids up

            //Assign points to the closest cluster
            retValue=assignCluster(centroids, clusterPoints, points, pointsPart, localCount, numNodes, iteration, clearIter);

            instance.getAtomicLong("assignsFinished").incrementAndGet();
            System.out.println("before whileee");
            while (instance.getAtomicLong("assignsFinished").get() != numNodes) {
                // while "assignClusters" not finished in all processes don't start "calculateCentroids"

            }
            System.out.println("after whileee");
            // As this call is between 2 waits for all processes is safe
            instance.getAtomicLong("iterationFinished").set(0);

            //Calculate new centroids.
            calculateCentroids(centroids, clusterPoints, clustersPart, localCount, numNodes, iteration, clearIter, instance);

            // Calculates total distance between new and old Centroids
            double distance = 0;
            int i = (int) ((localCount-1)*clustersPart);
            for (Point oldCentroid: lastCentroids ) {
                //Cluster c = (Cluster) clusters.get(i);
                Point currentCentroid = centroids.get(i);
                System.out.println("currentCentroid from "+i+" is= "+currentCentroid+" // oldCentroid: "+oldCentroid);
                distance += Point.distance(oldCentroid, currentCentroid);
                System.out.println("their D: "+Point.distance(oldCentroid, currentCentroid));
                System.out.println(" b4 i++ :"+i);
                i++;
            }

            System.out.println("distance before final: "+distance);
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

            System.out.println("ALL PROCESSES HAVE ADDED THEIR DISTANCES :"+ instance.getAtomicLong("distanceCompleted").get());

            if ( (double)instance.getAtomicReference("distance").get() == 0.0){
                System.out.println("GLOBAL DISTANCE = 0! "+instance.getAtomicReference("distance").get() + "in iteration: "+iteration);
                finish=true;
            }

            //finish= true;
            iteration++;
            System.out.println("ITERATION +1 !!!!!!!!!!!!!! "+iteration+" with a distance: "+instance.getAtomicReference("distance").get());
        }
        return retValue;
    }

    private static void clearClusters(MultiMap<Integer, Point> clusterPoints, int clustersPart, long localCount, int numNodes, long iteration, ConcurrentMap<Integer, Integer> clearIter, HazelcastInstance instance) {
        int module = 0;
        int clusterIter=0;

        System.out.println("multiMap size check "+clusterPoints.size());
        if (clusterPoints.size() != 0) {    // first iteration won't have any points yet

            if (localCount == numNodes) { // if it's last node
                module = clearIter.size() % numNodes;
            }

            for (int i = (int) ((localCount - 1) * clustersPart); i < ((localCount - 1) * clustersPart) + clustersPart + module; i++) {
                // walk through its part
                //Cluster c = (Cluster) clusters.get(i);
                clusterIter = clearIter.get(i);
                if (clusterIter < iteration) {   // if cluster needs to be cleared
                    if (clusterPoints.size() > 0) {
                        clusterPoints.remove(i);
                        System.out.println("clear Clusters: clusterPoints.containsKey(i) "+clusterPoints.containsKey(i));
                        //clusterPoints.
                        //clusterPoints.get(i).clear();
                    }
                    //c.clear();
                    clearIter.replace(i, (int) iteration);
                }
                System.out.println(localCount + ": cluster " + i + " cleared");
            }

        } else {System.out.println("clusterPoints size = 0 !!!!!!!!!!!! !!!!!!!!!!! !!!!!!!!!!!");}

    }
    private static void getLocalCentroids(ConcurrentMap<Integer, Point> centroids, int clustersPart, long localCount, int numNodes, List<Point> lastCentroids){
        int module = 0;

        lastCentroids.clear(); // avoids mixing centroids from different iterations

        if (localCount == numNodes) { // if it's last node
            module=centroids.size()%numNodes;
        }

        for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart + module; i++) {
            // walk through its part

            Point point = new Point();
            point.setX(centroids.get(i).getX());
            point.setY(centroids.get(i).getY());
            lastCentroids.add(point);

        }
    }

    private static int assignCluster(ConcurrentMap<Integer, Point> centroids,  MultiMap<Integer, Point> clusterPoints, ConcurrentMap<Integer, Point> points, int pointsPart, long localCount, int numNodes, long iteration, ConcurrentMap<Integer, Integer> clearIter) {
        double max = Double.MAX_VALUE;
        double min = max;
        int cluster = 0;
        double distance = 0.0;
        int repetitionMax;
        final int REPETITION_LIMIT = 200;
        int module = 0;
        List<Integer> delays = new ArrayList<>();

        System.out.println("second multiMap size check "+clusterPoints.size());


        if (localCount == numNodes) { // if it's last node
            module = points.size() % numNodes;
        }
        
        for (int i = (int) ((localCount - 1) * pointsPart); i < (localCount - 1) * pointsPart + pointsPart + module; i++) {     // for each point
            // walk through its part
            //Point point = (Point) points.get(i);
            min = max;

            for (int j = 0; j < centroids.size(); j++) {     // assign to the closest cluster
                //Cluster c = (Cluster) clusters.get(j);
                //System.out.println("clusterPoints.containsKey("+j+") "+clusterPoints.containsKey(j));
                if (clearIter.get(j) == iteration || iteration==1 || clusterPoints.containsKey(j) == false ) {    // if cluster has been cleared (first iter doesn't clear)
                    distance = Point.distance(points.get(i), centroids.get(j));
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

                //Cluster c = (Cluster) clusters.get(j);

                if (clearIter.get(j) == iteration || iteration==1 || clusterPoints.containsKey(j) == false ) { // // if cluster has been cleared
                    repetitionMax = REPETITION_LIMIT;
                    distance = Point.distance(points.get(i), centroids.get(j));
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
                points.get(i).setCluster(cluster);                          // mark point as ready for next stage (calculateCentroids)
                //Cluster aux = (Cluster) clusters.get(cluster);
            //    System.out.println("assignCluster1: cluster "+cluster+"  size: "+(clusterPoints.get(cluster).size())) ;
                clusterPoints.put(cluster, points.get(i));
                //clusterPoints.get(cluster).add(points.get(i)); //.addPoint(points.get(i));
                //clusters.get(cluster).points.add(points.get(i));
                //clusters.get(cluster).
            //    System.out.println("assignCluster2: cluster "+cluster+"  size: "+(clusterPoints.get(cluster).size())) ;




                //clusters.replace(cluster, aux);

                
            }

        }
        return 0;

    }

    private static void calculateCentroids(ConcurrentMap<Integer, Point> centroids,  MultiMap<Integer, Point> clusterPoints, int clustersPart, long localCount, int numNodes, long iteration, ConcurrentMap<Integer, Integer> clearIter, HazelcastInstance instance) {
        int module = 0;
        double sumX = 0;
        double sumY = 0;

        double newX = 0;
        double newY = 0;

        if (localCount == numNodes) { // if it's last node
            module=centroids.size()%numNodes;
        }

        for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart + module; i++) {      // for each cluster
            // walk through its part

            sumX=0;
            sumY=0;     // reset for each cluster

            //Cluster c = (Cluster) clusters.get(i);
            System.out.println("calcCent clusterPoints.get("+i+").size() :"+clusterPoints.get(i).size());
            for (Point point: clusterPoints.get(i) ) {               // for each of its points
                sumX += point.getX();                           // add to process local variables
                sumY += point.getY();                           // Todo: either use BigDecimal or check Double.POSITIVE_INFINITY or Double.NEGATIVE_INFINITY
            }

            Point centroid = centroids.get(i);
            int n_points = clusterPoints.get(i).size();
            if(n_points > 0) {
                System.out.println("points size bigger than 0");
                newX = sumX / n_points;                  // compute avg
                newY = sumY / n_points;

                centroid.setX(newX);                            // set clusters avg
                centroid.setY(newY);
                centroids.replace(i,centroid);
            } else{
                System.out.println("points size NOT NOT bigger than 0");
            }
            System.out.println(localCount+": centroid of cluster "+i+" calculated, AND is "+centroids.get(i));
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

        ConcurrentMap<Integer, Point> points = Point.createRandomPoints(minCoordinate, maxCoordinate, num_points, instance);
        ConcurrentMap<Integer,Integer> clearIter = instance.getMap("clearIter");        // Keeps track of the number of "clear" iterations of each cluster

        ConcurrentMap<Integer, Point> centroids = instance.getMap("centroids");
        MultiMap<Integer, Point> clusterPoints = instance.getMultiMap("clusterPoints");

       init(numClusters, minCoordinate, maxCoordinate, centroids, clearIter);

        IAtomicLong count = instance.getAtomicLong("count");
        long localCount = count.incrementAndGet();      // As new processes run, they increment a counter and keep the local copy as their ID
        if (localCount>numNodes){
            // Todo: create distributed long for numNodes and update it as needed
            System.out.println("number of nodes increased");
            return;
        }

        int pointsPart = points.size()/numNodes;
        int clustersPart = centroids.size()/numNodes;

        calculate(centroids, clusterPoints, points, clustersPart, pointsPart, localCount, numNodes, clearIter, instance); // main call

        finished.incrementAndGet(); // Counts finished processes
        while (finished.get() != numNodes){

        }


        long finalTime = System.currentTimeMillis();            // When lock released, time elapsed time
        debugEnd((finalTime-startTime)/1000, true, 0);    // Create a file with info about time (avoids busy st out)
        debugEnd(localCount, true, 0);
        //end(clusters);

    }

    public static void runSecondary(int numClusters, int num_points, int minCoordinate, int maxCoordinate, int numIter, int numNodes) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(15);
        clientConfig.getGroupConfig().setName("kmeansName").setPassword("kmeansPass");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);


        ConcurrentMap points = client.getMap("points");
        //ConcurrentMap clusters = client.getMap("clusters");
        ConcurrentMap<Integer,Integer> clearIter = client.getMap("clearIter");

        ConcurrentMap<Integer, Point> centroids = client.getMap("centroids");
        MultiMap<Integer, Point> clusterPoints = client.getMultiMap("clusterPoints");


        while(points.size()!=num_points || centroids.size() != numClusters || clearIter.size() != numClusters){
            // Wait for initialization
            // Todo: could be optimized putting thread to sleep, now hazelcast exception stops execution
        }
        IAtomicLong count = client.getAtomicLong("count");
        long localCount = count.incrementAndGet();
        if (localCount>numNodes){
            // Todo: create distributed long for numNodes and update it as needed
            System.out.println("number of nodes increased, localcount: "+ localCount+", numNodes: "+numNodes);
            return;
        }

        int pointsPart = points.size()/numNodes;
        int clustersPart = centroids.size()/numNodes;

        if (calculate(centroids, clusterPoints, points, clustersPart, pointsPart, localCount, numNodes, clearIter, client) ==0) {
            IAtomicLong finished = client.getAtomicLong("finished");
            finished.incrementAndGet();
           

            debugEnd(localCount, true, 0);
        }
        client.shutdown();
        //end(clusters);


    }

    // debugEnd debugs the execution of a process without using the (heavily used by Hazelcast) standard output
    public static void debugEnd(long localCount, boolean endSuccessful, int stoppedAt){
        String pid="_";
        if (endSuccessful){
            pid = String.valueOf(localCount)+"_OK";
        } else {
            pid = String.valueOf(localCount)+"KO!"+String.valueOf(stoppedAt);
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



