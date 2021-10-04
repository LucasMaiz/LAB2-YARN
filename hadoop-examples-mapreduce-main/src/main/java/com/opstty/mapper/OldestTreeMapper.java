package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



import java.io.IOException;

public class OldestTreeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String field = value.toString();

        if (!field.contains("ANNEE PLANTATION")){
            Text arron = new Text(value.toString().split(";")[1]);

            Text annee_str = new Text(value.toString().split(";")[5]);

            if(!annee_str.toString().isEmpty()){
                IntWritable annee = new IntWritable((int)Float.parseFloat(value.toString().split(";")[5]));
                context.write(arron, annee);
            }
        }
    }
}
