package com.scujcc.hadoop.one;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by David on 3/20/17.
 */
public class GradeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text wordOne = new Text();
    private DoubleWritable grade = null;

    @Override
    public void map(Object key, Text value, Context context){
        StringTokenizer itr = new StringTokenizer(value.toString());
        String strOne = itr.nextToken();
        String strTwo = itr.nextToken();
        wordOne.set(strOne);
        double d = Double.parseDouble(strTwo.trim());
        grade = new DoubleWritable(d);
        try {
            context.write(wordOne, grade);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
