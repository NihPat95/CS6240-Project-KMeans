package kmeans;

import org.apache.hadoop.conf.Configuration;
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
import java.io.IOException;
import java.util.List;


public class KMeans extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(KMeans.class);

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

        conf.set(Keys.INPUT_DATA_PATH, args[0]);
        conf.set(Keys.INPUT_CLUSTER_PATH, args[1]);
        conf.set(Keys.OUTPUT_PATH, args[2]);
        conf.setInt(Keys.MAX_ITERATION, Integer.valueOf(args[3]));
        conf.setDouble(Keys.ERROR, Double.parseDouble(args[4]));

        int maxIterations = Integer.parseInt(args[3]);
        int currentIteration = 0;

        while(currentIteration != maxIterations){

            System.out.println("Running iteration " + currentIteration);
            conf.setInt(Keys.ITERATION, currentIteration);
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
        FileInputFormat.addInputPath(job, new Path(conf.get(Keys.INPUT_DATA_PATH)));

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(Centroid.class);
        job.setMapOutputValueClass(Point.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

    }

    private void addCacheFiles(Configuration conf, Job job) throws IOException {
        int iteration = conf.getInt(Keys.ITERATION, 0);
        String BUCKET_NAME = "cs6240-k-means";
        System.out.println("Adding Cache for iteration - ");
        System.out.println(iteration);
        if (iteration > 1) {
            String dir_name = "output/" + Integer.toString(iteration - 1);
            List<String> files = FileUtility.getListOfFilesInDir(BUCKET_NAME, dir_name, true);
            System.out.println("Calling the above function");
            for(String file : files){
                Path path = new Path(file);
                job.addCacheFile(path.toUri());
                System.out.print("printing file ");
                System.out.println(file);
            }
//            System.out.println("Reaching here");
//            String output = conf.get(Keys.OUTPUT_PATH) + Keys.SEP + (iteration-1);
//            System.out.println("Printing output string");
//            System.out.println(output);
//            Path out = new Path(output, "part-r-[0-9]*");
//
//            System.out.println("printing path");
//            System.out.println(out.toString());
//
//            FileSystem fs = FileSystem.get(conf);
//            System.out.println("What is up1");
//            System.out.println(fs.toString());
//            FileStatus[] ls = fs.globStatus(out);
//            System.out.println("What is up2");
//            System.out.println("printing ls length");
//            System.out.println(ls.length);
//
//            for (FileStatus fileStatus : ls) {
//                System.out.println("JOR SE BOLO JMTD");
//                System.out.println(fileStatus);
//                Path pfs = fileStatus.getPath();
//                logger.info("Adding " + pfs.toUri().toString());
//                System.out.println("JOR SE BOLO HAR HAR GANGE");
//                System.out.println(pfs.toUri());
//                job.addCacheFile(pfs.toUri());
//            }
        }
        else {
            Path path = new Path(conf.get(Keys.INPUT_CLUSTER_PATH));
            System.out.println("First iteration adding " + path.toUri().toString());
            job.addCacheFile(path.toUri());
        }
    }

    private String getOutputPath(Configuration conf){
        return conf.get(Keys.OUTPUT_PATH) + Keys.SEP +
                (conf.getInt(Keys.ITERATION, 0));
    }

//    private void deleteOutputDirectory(Configuration conf) {
//        Path output = new Path(getOutputPath(conf));
//        try {
//            FileSystem dfs = FileSystem.get(conf);
//            if (dfs.isDirectory(output)) {
//                dfs.delete(output, true);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    private void run(Job job, Configuration conf) {
//        deleteOutputDirectory(conf);

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
