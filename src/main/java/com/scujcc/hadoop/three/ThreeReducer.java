package com.scujcc.hadoop.three;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by David on 4/10/17.
 */
public class ThreeReducer extends Reducer<Text, Text, Text, Text> {
    Text outputKey = new Text();
    Text outputValue = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String typeOfBook = "";
        List<String> infoOfStu = new ArrayList<String>();

        //test 查看reduce的输入
//        for (Text value : values) {
//            String val = value.toString();
//            System.out.print(val + " ");
//        }
//        System.out.println();

        for (Text value : values) {
            String val = value.toString();
            String[] varArray = val.split(" ");

            //test
            //System.out.print(val + " ");

            if (varArray.length == 1) {
                typeOfBook = val;
            } else {
                infoOfStu.add(val);
            }
        }
        //test
        //System.out.println();

        //test 查看 typeOfBook 和 infoOfStu
//        System.out.println("infoOfStu的值：");
//        for (int i = 0; i < infoOfStu.size(); i++) {
//            System.out.print(infoOfStu.get(i) + " ");
//        }
//        System.out.println();
//        System.out.println("typeOfBook的值：");
//        System.out.println(typeOfBook);

        for (int i = 0; i < infoOfStu.size(); i++) {
            outputKey.set(infoOfStu.get(i));
            outputValue.set(typeOfBook);
            context.write(outputKey, outputValue);
        }
    }
}
