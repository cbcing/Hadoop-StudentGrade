package com.scujcc.hadoop.six;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Created by David on 4/17/17.
 */
public class SixMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text keyEmit = new Text();
    Text valueEmit = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        StringTokenizer itr = new StringTokenizer(value.toString());
//        String strOne = itr.nextToken();
//        String strTwo = itr.nextToken();
//        keyEmit.set(strOne);
//        valueEmit.set(strTwo);
//        context.write(keyEmit, valueEmit);
        String str = value.toString();
        String[] strArr = str.split(" ");

        //test: show input of map
//        for (int i = 0; i < strArr.length; i++) {
//            System.out.print(strArr[i] + " ");
//        }
//        System.out.println();

        if (strArr.length == 2) {
            String keyEmitStr = strArr[0].concat(" ").concat(strArr[1]);
            String valueEmitStr = "";
            keyEmit.set(keyEmitStr);
            valueEmit.set(valueEmitStr);
            context.write(keyEmit, valueEmit);
        }
        if (strArr.length == 3) {
            String keyEmitStr = strArr[0].concat(" ").concat(strArr[1]);
            String valueEmitStr = strArr[2];
            keyEmit.set(keyEmitStr);
            valueEmit.set(valueEmitStr);
            context.write(keyEmit, valueEmit);
        }
    }
}
