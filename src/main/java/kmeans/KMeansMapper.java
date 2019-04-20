package kmeans;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class KMeansMapper extends Mapper<LongWritable, Text, Centroid, Point> {
    private Point point;
    private List<Centroid> centroids;

    @Override
    protected void setup(Context context) throws IOException {
        point = new Point();
        centroids = new ArrayList<>();

        URI[] urls = context.getCacheFiles();
        for (URI url: urls){

            FileSystem fs = FileSystem.get(context.getConfiguration());
            InputStreamReader reader = new InputStreamReader(fs.open(new Path(url)));
            BufferedReader br = new BufferedReader(reader);
            String line = br.readLine();

            // Reads all the center points from the cache file
            // and add to centroids list
            while (line != null){
                centroids.add(Centroid.parsePoints(line, ","));
                line = br.readLine();
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {
            // Parse the point
            point.parse(value.toString(),",");

            // Emit the closet centroid and the given point
            context.write(Centroid.closetPoint(centroids, point), point);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
