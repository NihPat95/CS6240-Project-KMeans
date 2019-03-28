package kmeans;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceTask extends Reducer <IntWritable, ArrayWritable, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<ArrayWritable> values, Context context) throws IOException, InterruptedException {
        /*
        # TODO for each key take the average of the points and emit
         */
    }
}
