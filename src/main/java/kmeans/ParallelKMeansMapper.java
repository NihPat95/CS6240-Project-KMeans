package kmeans;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.Key;
import java.util.*;

public class ParallelKMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
    private HashMap<Integer, Set<Point>> points;
    private List<Centroid> centroids;
    private int maxIteration;
    private double error;

    private static final Logger logger = LogManager.getLogger(ParallelKMeansMapper.class);

    @Override
    protected void setup(Context context) throws IOException {
        points = new HashMap<>();
        points.put(0, new HashSet<>());
        maxIteration = context.getConfiguration()
                .getInt(Keys.MAX_ITERATION, 1000);
        error = context.getConfiguration()
                .getDouble(Keys.ERROR, 0.01);

        URI[] urls = context.getCacheFiles();
        for (URI url: urls){

            FileSystem fs = FileSystem.get(context.getConfiguration());
            InputStreamReader reader = new InputStreamReader(fs.open(new Path(url)));
            BufferedReader br = new BufferedReader(reader);
            String line = br.readLine();

            // Read all data points into the Points list
            while (line != null){
                points.get(0).add(Point.parsePoint(line, ","));
                line = br.readLine();
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        int k = Integer.parseInt(value.toString());
        int count = 0;
        centroids = new ArrayList<>();
        logger.info("Map task of k = " + k);

        Iterator<Point> iterator = points.get(0).iterator();

        while (iterator.hasNext() && count < k){
                centroids.add(new Centroid(
                        new Text(""+count++), iterator.next()));
        }

        for(int i=0; i<k; i++){
            if(!points.containsKey(i))
                points.put(i, new HashSet<>());
        }


        int currentIteration = 0;
        boolean converge = false;
        while (currentIteration < maxIteration && !converge){
            // Assign points to clusters
            for(Integer label: points.keySet()){
                iterator = points.get(label).iterator();

                while (iterator.hasNext()){
                    Point p = iterator.next();

                    int l;
                    try {
                        l = Integer.parseInt(Centroid.closetPoint(centroids, p).getLabel().toString());
                    } catch (Exception e) {
                        l = 0;
                    }

                    if(l!=label){
                        iterator.remove();
                        points.get(l).add(p);
                    }

                }
            }

            // New Centeroid
            for (Integer label: points.keySet()){
                try {
                    Point avg = Point.getAverage(points.get(label));
                    if (Eucledian.getInstance()
                            .similarity(avg, centroids.get(label).getPoint()) < error){
                        converge = true;
                    }
                    centroids.get(label).setPoint(avg);
                } catch (Exception e) {
                    logger.error(e);
                }
            }
            currentIteration ++;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(k).append("\n");

        for(Centroid c:centroids){
            sb.append(Arrays.toString(c.getPoint().getVector()));
            sb.append("\n");
        }

        context.write(new Text(sb.toString()), null);
    }
}
