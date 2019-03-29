package kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class KMeans extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(KMeans.class);

    @Override
    public int run(String[] args) throws Exception {

        /*
        # TODO generate the initial k points for the first run
        # TODO write k points to file as id, d1, d2 ....
        # TODO distribute the c points file written as cache file
        # TODO loop the join in some iteration or till converge
        # TODO read and redistribute the c points from the previous iteration
         */

        //k is the number of clusters
        //d is vector dimension, in this case, number of movies
        double[][] initial_k = new double[k][d];

        File file = new File("path/to/file");
        file.createNewFile();
        BufferedWriter buffer_writer = new BufferedWriter(new FileWriter(file.getAbsoluteFile(), true));
        String data;
        //Calculating initial k points randomly and writing them to a file
        for(int i=0; i< k; i++){
            //generates a random rating from 0.0 to 5.0
            data = Integer.toString(i);
            buffer_writer.write(data);
            for(int j = 0; j < d; j++){
                double random = 0.0 + Math.random() * (5.0 - 1.0);
                k[i][j] = random;
                data = "," + Double.toString(k[i][j]);
                buffer_writer.write(data);
            }
        }


        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Kmeans");
        job.setJarByClass(KMeans.class);
        final Configuration jobConf = job.getConfiguration();

        job.setMapperClass(MapTask.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ArrayWritable.class);

        job.setReducerClass(ReduceTask.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        logger.info("End Job Config");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Three arguments required:\n" +
                    "output location, number of centers, number of dimension");
        }

        try {
            ToolRunner.run(new KMeans(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

    // returns the initial set of k x dimensions points
    public int[][] getInitialKpoints(int k, int dimensions) {

        return null;
    }

}
