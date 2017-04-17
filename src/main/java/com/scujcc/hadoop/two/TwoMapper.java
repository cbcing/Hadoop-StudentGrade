package com.scujcc.hadoop.two;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by David on 4/10/17.
 */
public class TwoMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        String strOne = itr.nextToken();
        word.set(strOne);
        try {
            context.write(word, one);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
