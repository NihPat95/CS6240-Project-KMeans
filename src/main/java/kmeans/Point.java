package kmeans;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Point implements Writable {
    private ArrayPrimitiveWritable vector = null;

    public Point(){
        vector = new ArrayPrimitiveWritable();
    }

    public Point(double[] vector) {
        this();
        setVector(vector);
    }

    public double[] getVector() {
        return (double[]) vector.get();
    }

    public void setVector(double[] vector) {
        this.vector.set(vector);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        vector.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        vector.readFields(dataInput);
    }

    @Override
    public String toString() {
        double[] thisVector = this.getVector();
        StringBuilder sb = new StringBuilder();
        for (int i = 0, j = thisVector.length; i < j; i++) {
            sb.append(thisVector[i]);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }

    public void parse(String values, String separator) {
        String[] coords = values.split(separator);
        double[] tmp = new double[coords.length-1];
        for (int i = 1; i < tmp.length; i++) {
            tmp[i-1] = Double.valueOf(coords[i]);
        }
        vector.set(tmp);
    }

    public void add(Point p) throws Exception {
        double[] current = getVector();
        double[] toAdd = p.getVector();

        if(current.length != toAdd.length)
            throw new Exception("Length must match");

        for(int i=0; i<current.length; i++)
            current[i] = current[i] + toAdd[i];

        this.setVector(current);
    }

    public void divide(int counter) {
        double[] current = getVector();
        for(int i=0; i<current.length; i++)
            current[i] = current[i] / counter;
        this.setVector(current);
    }
}
