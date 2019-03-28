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
