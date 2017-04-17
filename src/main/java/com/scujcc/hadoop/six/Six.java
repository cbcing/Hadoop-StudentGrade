package com.scujcc.hadoop.six;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by David on 4/17/17.
 */
/*
 * 用于处理three的输出。
 */
public class Six {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Grade <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(Six.class);
        job.setJobName("Six");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SixMapper.class);
        job.setReducerClass(SixReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
