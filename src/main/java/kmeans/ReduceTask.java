package kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceTask extends Reducer <IntWritable, ArrayWritable, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<ArrayWritable> values, Context context) throws IOException, InterruptedException {
        int size = (int)values.spliterator().getExactSizeIfKnown();
        double[] sum = new double[size];

        //For each value in the collection of distances
        for(ArrayWritable value : values){
           int i = 0;
           //Addition of each feature distance
           for(Writable writable : value.get()) {
               DoubleWritable doubleWritable = (DoubleWritable)writable;
               sum[i] += doubleWritable.get();
               i++;
           }
        }

        //Calculates the average of all the distances
        for(int i = 0;i<sum.length;i++){
            sum[i] = sum[i]/size;
        }

        //# TODO : Convert the value into Text and emit it
    }
}
