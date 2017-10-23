/**
 * Created by alvaro on 26/09/17.
 */
public class main {
    public static void main(String[] args) {
        int firstArg=1;
        if (args.length > 0) {
            try {
                firstArg = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Argument" + args[0] + " must be an integer.");
                System.exit(1);
            }
            final int NUM_CLUSTERS = 25;
            //Number of Points
            final int NUM_POINTS = 125;
            //Min and Max X and Y
            final int MIN_COORDINATE = 0;
            final int MAX_COORDINATE = 10000;

            final int NUM_ITER = 1;
            final int NUM_NODES = firstArg;

            parallelDistributed.KMeans.run(NUM_CLUSTERS,NUM_POINTS,MIN_COORDINATE,MAX_COORDINATE, NUM_ITER, NUM_NODES);

        } else {System.out.println("not enough args");}


    }
}
