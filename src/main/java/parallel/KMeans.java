package parallel;


import com.google.common.util.concurrent.AtomicDouble;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


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
    public static List<Cluster> init(int numClusters, int minCoordinate, int maxCoordinate) {
        //Create Clusters
        List<Cluster> clusters = new ArrayList();
        //Set Random Centroids
        for (int i = 0; i < numClusters; i++) {
            Cluster cluster = new Cluster(i);
            Point centroid = Point.createRandomPoint(minCoordinate,maxCoordinate);
            cluster.setCentroid(centroid);
            clusters.add(cluster);
        }
        return clusters;
        //Print Initial state
        //plotClusters();
    }
/*
    private void plotClusters() {
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            Cluster c = clusters.get(i);
            c.plotCluster();
        }
    }
*/
    //The process to calculate the K Means, with iterating method.
    public static void calculate(List<Cluster> clusters, List<Point> points) {
        boolean finish = false;
        int iteration = 0;

        // Add in new data, one at a time, recalculating centroids with each new one.
        while(!finish) {
            //Clear cluster state
            clearClusters(clusters);

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
            /*int i =0;
            lastCentroids.stream().parallel().forEach((Point lastCentroid) -> {
                distance.getAndAdd(Point.distance(lastCentroid, currentCentroids.get(i)));
                    i++;
            });
            */
            /*
            System.out.println("#################");
            System.out.println("Iteration: " + iteration);
            System.out.println("Centroid distances: " + distance);
            plotClusters();
            */

            if (distance.compareAndSet(0.0,0.0)){
                finish = true;
            }
        }
    }

    private static void clearClusters(List<Cluster> clusters) {
        for(Cluster cluster : clusters) {
            cluster.clear();
        }
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
            //clusters.get(cluster).addPoint(point);
        }
    }

    private static void calculateCentroids(List<Cluster> clusters) {
        //for(Cluster cluster : clusters) {           // parallelized
            clusters.stream().parallel().forEach((Cluster cluster) -> {
                /*double sumX = 0;
                double sumY = 0;*/
                AtomicDouble sumX2 = new AtomicDouble();
                AtomicDouble sumY2 = new AtomicDouble();

                List<Point> list = cluster.getPoints();
                int n_points = list.size();

                /*for(Point point : list) {               // Todo: can be divided in n processes
                    sumX += point.getX();
                    sumY += point.getY();
                }*/
                list.stream().parallel().forEach((Point point) -> {
                    sumX2.getAndAdd(point.getX());
                    sumY2.getAndAdd(point.getY());
                });
                /*System.out.println(sumX);
                System.out.println(sumX2);
                System.out.println(sumY);
                System.out.println(sumY2);
                */
                Point centroid = cluster.getCentroid();
                if(n_points > 0) {
                    /*double newX = sumX / n_points;
                    double newY = sumY / n_points;*/
                    AtomicDouble newX2 = new AtomicDouble();
                    AtomicDouble newY2 = new AtomicDouble();
                    newX2.set(sumX2.doubleValue()/n_points);
                    newY2.set(sumY2.doubleValue()/n_points);
                    /*System.out.println("newX  "+newX);
                    System.out.println("newX2 "+newX2);
                    System.out.println("newY  "+newY);
                    System.out.println("newY2 "+newY2);*/
                    centroid.setX(newX2.doubleValue());
                    centroid.setY(newY2.doubleValue());
                }
            });

        }

    public static void run(int numClusters, int num_points, int minCoordinate, int maxCoordinate, int numIter) {
        long startTime;
        long finalTime;

        long parallelTime=0;
        long imperativeTime=0;

        for (int i = 0; i <numIter ; i++) {

            List<Point> points = Point.createRandomPoints(minCoordinate, maxCoordinate, num_points);
            List<Cluster> clusters = init(numClusters, minCoordinate, maxCoordinate);

            List<Cluster> clustersOriginal = (List<Cluster>) DeepCopy.copy(clusters);
            List<Point> pointsOriginal = (List<Point>) DeepCopy.copy(points);



            startTime = System.currentTimeMillis();
            calculate(clusters, points);
            finalTime = System.currentTimeMillis();
            System.out.println(finalTime-startTime);
            parallelTime += (finalTime - startTime);

            startTime = System.currentTimeMillis();
            imperative.KMeans.calculate(clustersOriginal, pointsOriginal);
            finalTime = System.currentTimeMillis();
            System.out.println(finalTime-startTime);
            imperativeTime += (finalTime - startTime);

            end(clusters);
        }

        System.out.println("Parallel time: "+parallelTime/numIter + " ms");
        System.out.println("Imperative time: "+imperativeTime/numIter + " ms");

    }

    }



