package kmeans;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

public class MapTask extends Mapper<Object, Text, IntWritable, ArrayWritable> {
    public static double[][] C;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        //Getting Cache Files from Local Cache
        URI[] cacheFiles = context.getCacheFiles();

        //Returning Exception if no files are found to read
        if(cacheFiles == null || cacheFiles.length == 0) {
            throw new RuntimeException("User information is not set in DistributedCache");
        }

        //Reading the cache file to obtain k and d
        BufferedReader rdr = new BufferedReader(new FileReader("file name"));
        String[] parameters = rdr.readLine().split(",");
        int k = Integer.parseInt(parameters[0]);
        int d = Integer.parseInt(parameters[1]);
        C = new double[k][d];

        String line;
        String[] ratings;

        //Reading the cache file to populate C points
        while((line = rdr.readLine()) != null){
            ratings = line.split(",");
            for(int i = 0; i< k; i++){
                for(int j = 0; j<d; j++){
                    C[i][j] = Double.parseDouble(ratings[j]);
                }
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        String[] userRatings = value.toString().split(",");

        //Getting the user ID and ratings for that user from data
        int userId = Integer.parseInt(userRatings[0]);
        double[] ratings = new double[userRatings.length - 1];
        for(int i = 1;i<=userRatings.length;i++){
            ratings[i-1] = Double.parseDouble(userRatings[i]);
        }

        double min_distance = Double.MAX_VALUE;
        double min_index = 0;

        //Calculating distance of point with each cluster centre, i.e, each C value
        for(int i = 0; i<C.length;i++){
            double euc_distance = this.computeEuclideanDistance(C[i], ratings);
            if(euc_distance < min_distance){
                min_distance = euc_distance;
                min_index = i;
            }
        }

        //Return value of minimum C as key and its user Id as value
        // # TODO : Change it later to satisfy hadoop data types
        context.write(min_index, userId);

    }


    private double computeEuclideanDistance(double x[], double y[]){
        double distance = 0;
        if(x.length != y.length){
            System.out.print("DIMENSIONS MUST MATCH");
        }

        //Computes Euclidean Distance of two vectors
        for(int i = 0; i<x.length; i++){
            distance += Math.pow(x[i] - y[i], 2);
        }
        return Math.sqrt(distance);
    }

}
