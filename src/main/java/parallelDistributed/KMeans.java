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
    public static ConcurrentMap<Integer, Cluster> init(int numClusters, int minCoordinate, int maxCoordinate, HazelcastInstance instance, Map<Integer, Integer> clearIter) {
        // Create Clusters

        /*
        * As the elements of a plain Java List inside a Java Object cannot be modified concurrently
        * and the List cannot be implemented as a Hazelcast List (inside of an Object),
        * the Object Cluster is broken into 2 Hazelcast data structures referenced by the same key
        * ( ConcurrentMap for the field "centroid" (a Point) and MultiMap for the field "points" (a List of Points) )
        * */

        ConcurrentMap<Integer, Point> centroids = instance.getMap("centroids");
        MultiMap<Integer, Point> clusterPoints = instance.getMultiMap("clusterPoints");

        ConcurrentMap<Integer, Cluster> clusters = instance.getMap("clusters");
        for (int i = 0; i < numClusters; i++) {
            // Create and initialize new cluster


            Cluster cluster = new Cluster();
            cluster.setID(i);
            System.out.println("init 0");
            //cluster.setPoints(instance);
            System.out.println("init 1");

            // Set Random Centroids
            Point centroid = Point.createRandomPoint(minCoordinate,maxCoordinate);
            cluster.setCentroid(centroid);
            clusters.put(i, cluster);

            // Fills up clearIter entry
            clearIter.put(i,0);
        }
        return clusters;

    }

    // The process to calculate the K Means, with iterating method.
    public static int calculate(ConcurrentMap<Integer, Cluster> clusters, ConcurrentMap<Integer, Point> points, int clustersPart, int pointsPart, long localCount, int numNodes, ConcurrentMap<Integer, Integer> clearIter, HazelcastInstance instance) {
        boolean finish = false;
        long iteration = 1;
        int retValue=-1;
        List<Point> lastCentroids = new ArrayList<>();
        instance.getAtomicLong("resetDone").set(0);

        int module=0;
        if (localCount == numNodes) { // if it's last node
            module=clusters.size()%numNodes;
        }

        for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart + module; i++) {
            Cluster c = (Cluster) clusters.get(i);
            System.out.println("preCentroids: "+c.getCentroid());

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
            clearClusters(clusters, clustersPart, localCount, numNodes, iteration, clearIter, instance);

            // A copy of current centroids is saved in lastCentroids before they are recalculated
            getLocalCentroids(clusters, clustersPart, localCount, numNodes, lastCentroids);   // fills lastCentroids up

            //Assign points to the closest cluster
            retValue=assignCluster(clusters, points, pointsPart, localCount, numNodes, iteration, clearIter);

            instance.getAtomicLong("assignsFinished").incrementAndGet();

            while (instance.getAtomicLong("assignsFinished").get() != numNodes) {
                // while "assignClusters" not finished in all processes don't start "calculateCentroids"

            }

            // As this call is between 2 waits for all processes is safe
            instance.getAtomicLong("iterationFinished").set(0);

            //Calculate new centroids.
            calculateCentroids(clusters, points, clustersPart, localCount, numNodes, iteration, clearIter, instance);

            // Calculates total distance between new and old Centroids
            double distance = 0;
            int i = (int) ((localCount-1)*clustersPart);
            for (Point oldCentroid: lastCentroids ) {
                Cluster c = (Cluster) clusters.get(i);
                Point currentCentroid = c.getCentroid();
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

    private static void clearClusters(ConcurrentMap<Integer, Cluster> clusters, int clustersPart, long localCount, int numNodes, long iteration, ConcurrentMap<Integer, Integer> clearIter, HazelcastInstance instance) {
        int module = 0;
        int clusterIter=0;

        if (localCount == numNodes) { // if it's last node
            module=clusters.size()%numNodes;
        }

        for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart + module; i++) {
            // walk through its part
            //Cluster c = (Cluster) clusters.get(i);
            clusterIter = clearIter.get(i);
            if (clusterIter < iteration){   // if cluster needs to be cleared
                clusters.get(i).clear();
                //c.clear();
                clearIter.replace(i, clusterIter+1);
            }
            System.out.println(localCount+": cluster "+i+" cleared");
        }


    }
    private static void getLocalCentroids(ConcurrentMap<Integer, Cluster> clusters, int clustersPart, long localCount, int numNodes, List<Point> lastCentroids){
        int module = 0;

        lastCentroids.clear(); // avoids mixing centroids from different iterations

        if (localCount == numNodes) { // if it's last node
            module=clusters.size()%numNodes;
        }

        for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart + module; i++) {
            // walk through its part

            Point point = new Point();
            point.setX(clusters.get(i).getCentroid().getX());
            point.setY(clusters.get(i).getCentroid().getY());
            lastCentroids.add(point);


        }
    }

    private static int assignCluster(ConcurrentMap<Integer, Cluster> clusters, ConcurrentMap<Integer, Point> points, int pointsPart, long localCount, int numNodes, long iteration, ConcurrentMap<Integer, Integer> clearIter) {
        double max = Double.MAX_VALUE;
        double min = max;
        int cluster = 0;
        double distance = 0.0;
        int repetitionMax;
        final int REPETITION_LIMIT = 200;
        int module = 0;
        List<Integer> delays = new ArrayList<>();

        if (localCount == numNodes) { // if it's last node
            module = points.size() % numNodes;
        }
        
        for (int i = (int) ((localCount - 1) * pointsPart); i < (localCount - 1) * pointsPart + pointsPart + module; i++) {     // for each point
            // walk through its part
            //Point point = (Point) points.get(i);
            min = max;

            for (int j = 0; j < clusters.size(); j++) {     // assign to the closest cluster
                //Cluster c = (Cluster) clusters.get(j);

                if (clearIter.get(j) == iteration) {    // if cluster has been cleared
                    distance = Point.distance(points.get(i), clusters.get(j).getCentroid());
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

                if (clearIter.get(j) == iteration) { // // if cluster has been cleared
                    repetitionMax = REPETITION_LIMIT;
                    distance = Point.distance(points.get(i), clusters.get(j).getCentroid());
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
                System.out.println("assignCluster1: cluster "+cluster+"  size: "+(clusters.get(cluster).getPoints().size())) ;
                clusters.get(cluster).addPoint(points.get(i));
                clusters.get(cluster).points.add(points.get(i));
                //clusters.get(cluster).
                System.out.println("assignCluster2: cluster "+cluster+"  size: "+(clusters.get(cluster).getPoints().size())) ;


                clusters.get(cluster).points.add(points.get(i));
                //clusters.get(cluster).
                System.out.println("assignCluster3: cluster "+cluster+"  size: "+(clusters.get(cluster).getPoints().size())) ;

                //clusters.replace(cluster, aux);

                
            }

        }
        return 0;

    }

    private static void calculateCentroids(ConcurrentMap<Integer, Cluster> clusters, ConcurrentMap<Integer, Point> points, int clustersPart, long localCount, int numNodes, long iteration, ConcurrentMap<Integer, Integer> clearIter, HazelcastInstance instance) {
        int module = 0;
        double sumX = 0;
        double sumY = 0;

        double newX = 0;
        double newY = 0;

        if (localCount == numNodes) { // if it's last node
            module=clusters.size()%numNodes;
        }

        for (int i = (int) ((localCount-1)*clustersPart); i <((localCount-1)*clustersPart) + clustersPart + module; i++) {      // for each cluster
            // walk through its part

            sumX=0;
            sumY=0;     // reset for each cluster

            //Cluster c = (Cluster) clusters.get(i);
            for (Point point: clusters.get(i).points) {               // for each of its points
                sumX += point.getX();                           // add to process local variables
                sumY += point.getY();                           // Todo: either use BigDecimal or check Double.POSITIVE_INFINITY or Double.NEGATIVE_INFINITY
            }

            Point centroid = clusters.get(i).getCentroid();
            int n_points = clusters.get(i).points.size();
            if(n_points > 0) {
                System.out.println("points size bigger than 0");
                newX = sumX / n_points;                  // compute avg
                newY = sumY / n_points;

                centroid.setX(newX);                            // set clusters avg
                centroid.setY(newY);
                clusters.get(i).setCentroid(centroid);
            } else{
                System.out.println("points size NOT NOT bigger than 0");
            }
            System.out.println(localCount+": centroid of cluster "+i+" calculated, AND is "+clusters.get(i).getCentroid());
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
        ConcurrentMap<Integer, Cluster> clusters = init(numClusters, minCoordinate, maxCoordinate, instance, clearIter);

        IAtomicLong count = instance.getAtomicLong("count");
        long localCount = count.incrementAndGet();      // As new processes run, they increment a counter and keep the local copy as their ID
        if (localCount>numNodes){
            // Todo: create distributed long for numNodes and update it as needed
            System.out.println("number of nodes increased");
            return;
        }

        int pointsPart = points.size()/numNodes;
        int clustersPart = clusters.size()/numNodes;

        calculate(clusters, points, clustersPart, pointsPart, localCount, numNodes, clearIter, instance); // main call

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
        ConcurrentMap clusters = client.getMap("clusters");
        ConcurrentMap<Integer,Integer> clearIter = client.getMap("clearIter");


        while(points.size()!=num_points || clusters.size() != numClusters || clearIter.size() != numClusters){
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
        int clustersPart = clusters.size()/numNodes;

        if (calculate(clusters, points, clustersPart, pointsPart, localCount, numNodes, clearIter, client) ==0) {
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



