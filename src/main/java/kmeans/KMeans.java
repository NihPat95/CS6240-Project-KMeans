package kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

public class KMeans extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(KMeans.class);

    @Override
    public int run(String[] args) throws Exception {

        /*
        # TODO loop the join in some iteration or till converge
        # TODO read and redistribute the c points from the previous iteration
        # TODO run on aws
         */

        //k is the number of clusters
        //d is vector dimension, in this case, number of movies
        // # TODO Set values for k and d later
        int k = 4;
        int d = 2;

        logger.info("Start Job Config");
        //String centersFilePath = writeKpoints(k, d,"centers");
        logger.info("Centers file path is " + "/centers");

        final Configuration conf = getConf();
        conf.set("K", String.valueOf(k));
        conf.set("D", String.valueOf(d));

        final Job job = Job.getInstance(conf, "Kmeans");
        job.setJarByClass(KMeans.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(MapTask.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);

        job.setReducerClass(ReduceTask.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("/Users/nihpat95/Documents/CS6240-Project-KMeans/centers"));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        logger.info("End Job Config");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n" +
                    "input folder, output folder");
        }

        try {
            ToolRunner.run(new KMeans(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

    public String writeKpoints(int k, int d, String filePath) throws IOException {
        File file = new File(filePath);
        file.createNewFile();
        BufferedWriter buffer_writer = new BufferedWriter(new FileWriter(file.getAbsoluteFile(), true));

        //Calculating initial k points randomly and writing them to a file
        for(int i=0; i < k; i++){
            StringBuilder sb = new StringBuilder();
            for( int j = 0; j < d; j++){
                //generates a random rating from 0.0 to 5.0
                double random = Math.random() * 5.0;
                sb.append(random).append(",");
            }

            sb.deleteCharAt(sb.length()-1);
            sb.append("\n");
            buffer_writer.write(sb.toString());
            logger.info(sb.toString());
            buffer_writer.flush();
        }
        return file.getAbsolutePath();
    }

}
