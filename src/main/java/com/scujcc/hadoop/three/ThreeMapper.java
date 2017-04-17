package com.scujcc.hadoop.three;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by David on 4/10/17.
 */
public class ThreeMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text keyEmit = new Text();
    Text valueEmit = new Text();

    private static int test = 1;
    private static int count = 1;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(" ");

        //test
        if (test == 1) {
            System.out.println("查看Mapper1处理的数据：");
            for (int i = 0; i < parts.length; i++) {
                System.out.print(parts[i] + " ");
            }
            System.out.println();
            System.out.println("这里是Mapper1");
            test++;
        }

        //test 查看Mapper1的所有输入数据
//        for(int i = 0; i < parts.length; i++) {
//            System.out.print(parts[i] + " ");
//        }
//        System.out.println();

        //test 测试map函数执行的次数
//        System.out.println(count);
//        count++;

        if (parts.length == 3) {
            String year = parts[2].split("-")[0];
            String stuNumAndYear = parts[0].concat(" ").concat(year);
            //test
            if (test == 2) {
                System.out.println(parts[1] + " " + stuNumAndYear);
                test++;
            }

            keyEmit.set(parts[1]);
            valueEmit.set(stuNumAndYear);
            context.write(keyEmit, valueEmit);
        }

    }
}
