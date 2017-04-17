package com.scujcc.hadoop.one;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by David on 3/20/17.
 */

/*
 * 统计学生成绩，类似于WorldCount
 */
public class Grade {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Grade <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(Grade.class);
        job.setJobName("Grade");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(GradeMapper.class);
        job.setReducerClass(GradeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
