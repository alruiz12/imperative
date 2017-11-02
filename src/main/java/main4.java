/**
 * Created by alvaro on 26/09/17.
 */
public class main4 {
    public static void main(String[] args) {
        int firstArg = 1;
        if (args.length > 0) {
            try {
                firstArg = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Argument" + args[0] + " must be an integer.");
                System.exit(1);
            }
            final int NUM_CLUSTERS = 150;
            //Number of Points
            final int NUM_POINTS = 1500;
            //Min and Max X and Y

            final int NUM_NODES = firstArg;

            networkImproved.KMeans.runSecondary(NUM_CLUSTERS, NUM_POINTS, NUM_NODES);

        } else {System.out.println("not enough args");}
    }
}
