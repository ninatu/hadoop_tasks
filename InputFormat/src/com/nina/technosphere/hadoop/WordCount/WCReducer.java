package com.nina.technosphere.hadoop.WordCount;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configured;

/**
 * Created by nina on 13.10.16.
 */

public class WCReducer
        extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    public void reduce(Text word, Iterable<LongWritable> counts, Context context) {
        long sum = 0;
        for(LongWtitable count : counts) {
            sum += count.get();
        }
        context.write(word, new LongWritable(sum));
    }
}