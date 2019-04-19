package kmeans;

import com.sun.istack.NotNull;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/***
 * Class to denote the center points
 * Label - id of the center point
 * Point - the actual features of the point
 */
public class Centroid implements WritableComparable<Centroid> {
    private Text label;
    private Point point;

    public Centroid() {
        label = new Text();
        point = new Point();
    }

    public Centroid(Text label, Point point) {
        this.label = label;
        this.point = point;
    }

    public Text getLabel() {
        return label;
    }

    public void setLabel(Text label) {
        this.label = label;
    }

    public Point getPoint() {
        return point;
    }

    public void setPoint(Point point) {
        this.point = point;
    }

    @Override
    public int compareTo(@NotNull Centroid centroid) {
        return this.label.compareTo(centroid.getLabel());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        label.write(dataOutput);
        point.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        label.readFields(dataInput);
        point.readFields(dataInput);
    }

    @Override
    public String toString() {
        return this.label + "," + this.point.toString();
    }

    /***
     * Given a string line returns the centroid point parsing the line
     * example format - id,p1,p2,...,pd
     * @param line String line containing id <separator> points
     * @param separator Regex used to split the input line
     * @return
     */
    public static Centroid parsePoints(String line, String separator) {
        String[] ids = line.split(separator);
        Text label = new Text(ids[0]);

        double[] points = new double[ids.length-1];
        for(int i=1; i<ids.length; i++){
            points[i-1] = Double.parseDouble(ids[i]);
        }

        return new Centroid(label, new Point(points));
    }

    /***
     * Given a point and set of centroid points
     * returns the closest centroid point based on the
     * eucledian distance
     * @param centroids set of centroid points
     * @param point point to find the closet distance to centroid
     * @return
     * @throws Exception
     */

    public static Centroid closetPoint(List<Centroid> centroids, Point point)
            throws Exception {

        Centroid result = null;
        double bestSimilarity = Double.MAX_VALUE;
        for(Centroid centroid: centroids){
            double similarity = Eucledian.getInstance()
                    .similarity(centroid.getPoint(), point);
            if (similarity < bestSimilarity){
                bestSimilarity = similarity;
                result = centroid;
            }
        }
        return result;
    }

}
