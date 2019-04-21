package kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ParallelKMeans extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(ParallelKMeans.class);

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        /**
            # argument 0 - path to input data points
            # argument 1 - path to file containing different k values
            # argument 2 - path to write the output for iteration
            # argument 3 - max iterations to run
            # argument 4 - max error between two iterations
        */

        conf.set(Keys.INPUT_DATA_PATH, args[0]);
        conf.set(Keys.INPUT_KVALUE_PATH, args[1]);
        conf.set(Keys.OUTPUT_PATH, args[2]);
        conf.setInt(Keys.MAX_ITERATION, Integer.valueOf(args[3]));
        conf.setDouble(Keys.ERROR, Double.parseDouble(args[4]));

        final Job job = Job.getInstance(conf, "ParallelKmeans");

        job.setJarByClass(getClass());
        job.addCacheFile(new Path(args[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        NLineInputFormat.setNumLinesPerSplit(job,1);
        job.setMapperClass(ParallelKMeansMapper.class);
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {

        if (args.length != 5) {
            throw new Error("Invalid input arguments \n" +
                    "# argument 0 - path to input data points\n" +
                    "# argument 1 - path to file containing different k values\n" +
                    "# argument 2 - path to write the output for iteration n\n" +
                    "# argument 3 - max iterations to run\n" +
                    "# argument 4 - max error between two iterations");
        }

        try {
            ToolRunner.run(new ParallelKMeans(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
