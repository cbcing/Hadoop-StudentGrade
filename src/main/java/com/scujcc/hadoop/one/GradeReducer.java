package com.scujcc.hadoop.one;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by David on 3/20/17.
 */
public class GradeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> value, Context context)
            throws IOException {
        double sum = 0;
        int count = 0;
        Iterator itr = value.iterator();
        while(itr.hasNext()){
            sum += (Double) itr.next();
            count++;
        }
        try {
            context.write(key, new DoubleWritable(sum/count));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
