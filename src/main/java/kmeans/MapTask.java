package kmeans;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapTask extends Mapper<Object, Text, IntWritable, ArrayWritable> {
    @Override

    protected void setup(Context context) throws IOException, InterruptedException {
        // # TODO read from the cache file and populate the C points
        // # TODO read from the cache file parameter k - points and d - dimensions
        super.setup(context);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        /*
        # TODO compare each point with points in C and emit the index of minimum with the point value
         */

        super.map(key, value, context);
    }


    private double computeEuclideanDistance(int x[], int y[]){
        /*
        # TODO return the euclidean distance between two points, dimension of x y must match
         */
        return 0;
    }

}
