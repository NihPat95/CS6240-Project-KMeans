package kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.Arrays;

public class MapTask extends Mapper<Object, Text, IntWritable, DoubleArrayWritable> {

    public static double[][] centerPoints;
    int k;
    int d;
    private static final Logger logger = LogManager.getLogger(MapTask.class);


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        //Getting Cache Files from Local Cache
        URI[] cacheFiles = context.getCacheFiles();

        //Returning Exception if no files are found to read
        if (cacheFiles == null || cacheFiles.length == 0) {
            throw new RuntimeException("Center points are not in the cache file");
        }

        logger.info("Cache file path: " + cacheFiles[0].getPath());

        //Reading the cache file to obtain k and d
        BufferedReader rdr = new BufferedReader(new FileReader("centers"));

        k = Integer.parseInt(context.getConfiguration().get("K"));
        d = Integer.parseInt(context.getConfiguration().get("D"));

        centerPoints = new double[k][d];

        String line;
        String[] ratings;
        int l = 0;
        //Reading the cache file to populate centerPoints points
        while((line = rdr.readLine()) != null && l<k){
            ratings = line.split(",");
            for(int j = 0; j < d; j++){
                centerPoints[l][j] = Double.parseDouble(ratings[j]);
            }
            l++;
        }

        logger.info("PRint C");
        for (int i = 0; i < k; i++) {
            logger.info(Arrays.toString(centerPoints[i]));
        }
        logger.info("PRint End");


    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] userRatings = value.toString().split(",");
        //Getting the user ID and ratings for that user from data
        double userId = Double.parseDouble(userRatings[0]);

        if (d != userRatings.length - 1) {
            throw new Error("The number of user feature must be " + d);
        }

        double[] ratings = new double[d];
        for (int i = 1; i < userRatings.length; i++) {
            ratings[i - 1] = Double.parseDouble(userRatings[i]);
        }

        double minDistance = Double.MAX_VALUE;
        int minIndex = -1;

        //Calculating distance of point with each cluster centre, i.e, each centerPoints value
        for (int i = 0; i < centerPoints.length; i++) {
            logger.info(centerPoints.length);
            double euclideanDistance = computeEuclideanDistance(centerPoints[i], ratings);

            logger.info(String.format("%s, %s, %f", Arrays.toString(centerPoints[i]), Arrays.toString(ratings), euclideanDistance));

            if (euclideanDistance < minDistance) {
                logger.info("new index " + i);
                minDistance = euclideanDistance;
                minIndex = i;
            }
        }


        //Return value of minimum centerPoints as key and its user Id as value
        context.write(new IntWritable(minIndex), new DoubleArrayWritable(ratings));
    }


    private double computeEuclideanDistance(double x[], double y[]) {
        double distance = 0;

        //Computes Euclidean Distance of two vectors
        for (int i = 0; i < x.length; i++) {
            distance += Math.pow(x[i] - y[i], 2);
        }
        return Math.sqrt(distance);
    }

}
