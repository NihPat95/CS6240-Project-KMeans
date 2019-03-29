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
    @Override

    public static double[][] C;
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
        /*
        # TODO compare each point with points in C and emit the index of minimum with the point value
         */

        super.map(key, value, context);
    }


    private double computeEuclideanDistance(int x[], int y[]){
        /*c
        # TODO return the euclidean distance between two points, dimension of x y must match
         */
        return 0;
    }

}
