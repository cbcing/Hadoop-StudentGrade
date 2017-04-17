package com.scujcc.hadoop.three;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by David on 4/10/17.
 */
/*
 * mapreduce 处理多个文件的输入。
 */
public class Three extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage : <inputlocation>  <inputlocation>  <outputlocation> ");
            System.exit(0);
        }
        int res = ToolRunner.run(new Configuration(), new Three(), args);
        System.exit(res);
    }

    public int run(String[] strings) throws Exception {
        /*
        String source1 = strings[0];
        String source2 = strings[1];
        String dest = strings[2];

        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", " ");

        FileSystem fileSystem = FileSystem.get(configuration);
        Job job = new Job(configuration, "Multiple Jobs");

        job.setJarByClass(Three.class);
        MultipleInputs.addInputPath(job, new Path(source1), TextInputFormat.class, ThreeMapper.class);
        MultipleInputs.addInputPath(job, new Path(source2), TextInputFormat.class, ThreeMapperTwo.class);
        job.setReducerClass(ThreeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        Path out = new Path(dest);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }

        TextOutputFormat.setOutputPath(job, out);
        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
        */
        Configuration configuration = new Configuration();
        String[] files = new  GenericOptionsParser(configuration, strings).getRemainingArgs();
        Path p1 = new Path(files[0]);
        Path p2 = new Path(files[1]);
        Path p3 = new Path(files[2]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(p3)) {
            fileSystem.delete(p3, true);
        }
        Job job = new Job(configuration, "Multiple Job");
        job.setJarByClass(Three.class);
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, ThreeMapper.class);
        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, ThreeMapperTwo.class);
        job.setReducerClass(ThreeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, p3);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
