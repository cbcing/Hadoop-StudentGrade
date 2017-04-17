package com.scujcc.hadoop.six;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by David on 4/17/17.
 */
public class SixReducer extends Reducer<Text, Text, Text, Text> {
    Text valueEmit = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        String valueEmitArr = "";
        for (Text val : value) {
            String str = val.toString();
            valueEmitArr.concat(str).concat(" ");
        }
        valueEmit.set(valueEmitArr);
        context.write(key, valueEmit);
    }
}
