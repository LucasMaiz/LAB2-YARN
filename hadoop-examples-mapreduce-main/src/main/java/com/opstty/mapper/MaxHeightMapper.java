package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String field = value.toString();

        if (!field.contains("GENRE")){
            Text speciesKey = new Text(value.toString().split(";")[2]);

            Text height_str = new Text(value.toString().split(";")[6]);

            if(!height_str.toString().isEmpty()){
                IntWritable height = new IntWritable((int)Float.parseFloat(value.toString().split(";")[6]));
                context.write(speciesKey, height);
            }
        }
    }
}
