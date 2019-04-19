package kmeans;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Arrays;


public class KMeans extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(KMeans.class);

    private static final String INPUT_DATA_PATH = "inputDataPath";
    private static final String INPUT_CLUSTER_PATH = "inputClusterPath";
    private static final String OUTPUT_PATH = "outputPath";
    private static final String MAX_ITERATION = "maxEpochs";
    private static final String ITERATION = "iteration";
    private static final String ERROR = "error";
    private static final String STOP = "stop";
    private static final String SEP = System.getProperty("file.separator");

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        /*
            # argument 0 - path to input data points
            # argument 1 - path to input cluster data
            # argument 2 - path to write the output for iteration n
            # argument 3 - max iterations to run
            # argument 4 - max error between two iterations
        */

        System.out.println(Arrays.toString(args));

        conf.set(INPUT_DATA_PATH, args[0]);
        conf.set(INPUT_CLUSTER_PATH, args[1]);
        conf.set(OUTPUT_PATH, args[2]);
        conf.setInt(MAX_ITERATION, Integer.valueOf(args[3]));
        conf.setDouble(ERROR, Double.parseDouble(args[4]));

        int maxIterations = Integer.parseInt(args[3]);
        int currentIteration = 0;

        while(currentIteration != maxIterations){

            System.out.println("Running iteration " + currentIteration);
            conf.setInt(ITERATION, currentIteration);
            Job job = Job.getInstance(conf, "Kmeans");
            execute(conf, job);
            if(job.getCounters().findCounter(Counter.STOPCOUNTER).getValue() == 1) {
                logger.info("Stopping because of convergence");
                break;
            }
            currentIteration++;
        }

        return 0;
    }

    private void execute(Configuration conf, Job job) {

        try {
            setJobConf(conf, job);
            run(job, conf);
        } catch (IOException e) {
            logger.error("Error executing current iterator", e);
        }


    }

    private void setJobConf(Configuration conf, Job job) throws IOException {

        job.setJarByClass(getClass());
        addCacheFiles(conf, job);

        FileOutputFormat.setOutputPath(job, new Path(getOutputPath(conf)));
        FileInputFormat.addInputPath(job, new Path(conf.get(INPUT_DATA_PATH)));

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(Centroid.class);
        job.setMapOutputValueClass(Point.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

    }

    private void addCacheFiles(Configuration conf, Job job) throws IOException {
        int iteration = conf.getInt(ITERATION, 0);

        if (iteration > 1) {

            String output = conf.get(OUTPUT_PATH) + SEP + (iteration-1);

            Path out = new Path(output, "part-r-[0-9]*");

            FileSystem fs = FileSystem.get(conf);
            FileStatus[] ls = fs.globStatus(out);
            for (FileStatus fileStatus : ls) {
                Path pfs = fileStatus.getPath();
                logger.info("Adding " + pfs.toUri().toString());
                job.addCacheFile(pfs.toUri());
            }
        }
        else {
            Path path = new Path(conf.get(INPUT_CLUSTER_PATH));
            logger.info("First iteration adding " + path.toUri().toString());
            job.addCacheFile(path.toUri());
        }
    }

    private String getOutputPath(Configuration conf){
        return conf.get(OUTPUT_PATH) + SEP + (conf.getInt(ITERATION, 0));
    }

    private void deleteOutputDirectory(Configuration conf) {
        Path output = new Path(getOutputPath(conf));
        try {
            FileSystem dfs = FileSystem.get(conf);
            if (dfs.isDirectory(output)) {
                dfs.delete(output, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void run(Job job, Configuration conf) {
        deleteOutputDirectory(conf);

        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(final String[] args) {

        if (args.length != 5) {
            throw new Error("Invalid input arguments \n" +
                    "# argument 0 - path to input data points\n" +
                    "# argument 1 - path to input cluster data\n" +
                    "# argument 2 - path to write the output for iteration n\n" +
                    "# argument 3 - max iterations to run\n" +
                    "# argument 4 - max error between two iterations");
        }

        try {
            ToolRunner.run(new KMeans(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
