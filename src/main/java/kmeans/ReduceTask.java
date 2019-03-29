package kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ReduceTask extends Reducer <IntWritable, DoubleArrayWritable, IntWritable, Text> {

    private static final Logger logger = LogManager.getLogger(ReduceTask.class);


    @Override
    protected void reduce(IntWritable key, Iterable<DoubleArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;

        int k = Integer.parseInt(context.getConfiguration().get("K"));
        int d = Integer.parseInt(context.getConfiguration().get("D"));

        double[] sum = new double[d];

        //For each value in the collection of distances
        for(DoubleArrayWritable value : values){
           count = count + 1;

           double[] ratings = value.getValues();

           if(ratings.length != d){
               throw new Error(String.format("Dimension does not match %d, %d", ratings.length, d));
           }

           //Addition of each feature distance
           for(int i=0; i<d; i++) {
               sum[i] = sum[i] + ratings[i];
           }
        }

        //Calculates the average of all the distances
        for(int i = 0; i<d; i++){
            sum[i] = sum[i]/count;
        }

        StringBuilder sb = new StringBuilder();

        for(int i=0; i<d; i++){
            sb.append(sum[i]).append(",");
        }

        sb.deleteCharAt(sb.length()-1);
        sb.append("\n");
        context.write(key, new Text(sb.toString()));
    }
}
