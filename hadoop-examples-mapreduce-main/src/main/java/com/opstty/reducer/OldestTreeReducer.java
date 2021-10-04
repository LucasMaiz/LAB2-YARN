package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Calendar;

public class OldestTreeReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    private IntWritable minage = new IntWritable();

    public void reduce(Text arrondissementKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int min = 5000;

        for (IntWritable valspe : values) {
            if (valspe.get() < min) {
                min = valspe.get();
            }
        }
        System.out.print(arrondissementKey + " : " + min);
        Calendar c = Calendar.getInstance();
        int year = c.get(Calendar.YEAR);
        minage.set(year - min);
        context.write(arrondissementKey, minage);

    }
}
