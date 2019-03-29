package kmeans;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class DoubleArrayWritable implements Writable {

    private double[] values;
    private int length;

    public DoubleArrayWritable(){
        super();
    }

    public DoubleArrayWritable(double[] values) {
        this.values = values;
        this.length = values.length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(length);
        for (int i = 0; i < length; i++) {
            out.writeDouble(values[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.length = in.readInt();
        values = new double[length];
        for (int i = 0; i < length; i++) {
            values[i] = in.readDouble();
        }
    }

    public double[] getValues() {
        return values;
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }
}
