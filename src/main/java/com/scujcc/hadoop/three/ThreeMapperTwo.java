package com.scujcc.hadoop.three;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by David on 4/10/17.
 */
public class ThreeMapperTwo extends Mapper<LongWritable, Text, Text, Text> {
    Text keyEmit = new Text();
    Text valueEmit = new Text();

    private static int test = 2;

    private static int count = 1;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //String line = value.toString();
        //String[] parts = line.split(" ");

        StringTokenizer itr = new StringTokenizer(value.toString());
        keyEmit.set(itr.nextToken());
        valueEmit.set(itr.nextToken());

        //test 展示keyEmit和valueEmit的值
        System.out.println(keyEmit + " " + valueEmit);

//        for (int i = 0; i < parts.length; i++) {
//            System.out.print(parts[i] + " ");
//        }
//        System.out.println();

        //test
//        if (test == 2){
//            System.out.println("查看Mapper2数据：");
//            for (int i = 0; i < parts.length; i++){
//                System.out.print(parts[i] + " ");
//            }
//            System.out.println();
//            System.out.println("这是ThreeMapperTwo!");
//            test++;
//        }

        //test 测试Mapper2的输入数据

        //test 测试map()函数执行的次数
//        System.out.println(count);
//        count++;

        //if (parts.length == 2){
            //keyEmit.set(parts[0]);
            //valueEmit.set(parts[1]);
            //test

            context.write(keyEmit, valueEmit);
        //}

    }
}
