/**
 * Created by alvaro on 26/09/17.
 */
public class main {
    public static void main(String[] args) {
        final int NUM_CLUSTERS = 45;
        //Number of Points
        final int NUM_POINTS = 100;
        //Min and Max X and Y
        final int MIN_COORDINATE = 0;
        final int MAX_COORDINATE = 10000;

        final int NUM_ITER = 1;
        final int NUM_NODES = 6;

        parallelDistributed.KMeans.run(NUM_CLUSTERS,NUM_POINTS,MIN_COORDINATE,MAX_COORDINATE, NUM_ITER, NUM_NODES);
    }
}
