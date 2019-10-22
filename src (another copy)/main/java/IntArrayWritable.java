import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.DataOutput;
import java.io.IOException;

public class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable(IntWritable[] intWritables) {
        super(IntWritable.class);
    }

    @Override
    public IntWritable[] get() {
        return (IntWritable[]) super.get();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        for (IntWritable data : get()) {
            data.write(arg0);
        }
    }
}