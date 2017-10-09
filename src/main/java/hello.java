/**
 * Created by alvaro on 26/09/17.
 */
public class hello {
    public static void main(String[] args) {
        final int NUM_CLUSTERS = 20;
        //Number of Points
        final int NUM_POINTS = 400;
        //Min and Max X and Y
        final int MIN_COORDINATE = 0;
        final int MAX_COORDINATE = 10000;

        final int NUM_ITER = 1;

        long startTime;
        long finalTime;

        //long imperativeTime=0;
        long parallelTime=0;

        for (int i = 0; i <1 ; i++) {/*
            startTime=System.currentTimeMillis();
            //imperative.KMeans.run(NUM_CLUSTERS, NUM_POINTS,MIN_COORDINATE,MAX_COORDINATE);
            finalTime=System.currentTimeMillis();
            imperativeTime += (finalTime-startTime);
*/
            startTime=System.currentTimeMillis();
            //parallel.KMeans.run(NUM_CLUSTERS,NUM_POINTS,MIN_COORDINATE,MAX_COORDINATE, NUM_ITER);
            finalTime=System.currentTimeMillis();
            parallelTime += (finalTime-startTime);
        }
        //System.out.println("Imperative time: "+imperativeTime/NUM_ITER + " ms");
        //System.out.println("Parallel time: "+parallelTime/NUM_ITER + " ms");

    }
}
