package kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class KMeansReducer extends Reducer<Centroid, Point, Text, NullWritable> {
    private Double delta = 0.;
    private static final String ERROR = "error";
    private static final Logger logger = LogManager.getLogger(KMeansReducer.class);

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();

        //  error for convergence
        delta = conf.getDouble(ERROR, 0.001);
    }

    @Override
    protected void reduce(Centroid key, Iterable<Point> values, Context context) {

        Point averagePoint = null;
        try {
            // get the average of all the points in one cluster
            averagePoint = getAverage(values);
        } catch (Exception e) {
            logger.info("Error");
        }

        try {

            double similarity = Eucledian.getInstance()
                    .similarity(key.getPoint(), averagePoint);

            // check for convergence
            if (similarity < delta){
                context.getCounter(Counter.STOPCOUNTER).setValue(1);
            }

            key.setPoint(averagePoint);
            context.write(new Text(key.toString()), NullWritable.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Returns the average of the all the points
    // The dimension of all points must match
    private Point getAverage(Iterable<Point> values) throws Exception {

        Point result = null;
        int counter = 0;

        for (Point p : values) {
            if (result == null) {
                result = p;
            } else {
                result.add(p);
            }
            counter++;
        }

        result.divide(counter);
        return result;
    }


}
