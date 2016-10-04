package com.nina.technosphere.hadoop;

/**
 * Created by nina on 04.10.16.
 */

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.regex.Pattern;

public class WordCount extends Configured implements Tool {

    public static void main(String[] args) {
        int res = ToolRunner.run(new WordCount(), args);
        System.exit(res);
    }

    public int run(String [] args) throws Exception {
        Job job = Job.getInstance(getConf(), "wordcount");
        job.setJarByClass(this.getClass());

        job.setMapperClass(WCMapper.class);
        job.setReduserClass(WCReducer.class);
        job.setOuputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class WCMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private final static Pattern word_pattern = Pattern.compile("\\p{L}+");

        @Override
        public void map(LongWritable key, Text text, Context context)
        {
            String line = text.toString();
            for (String curWord: word_pattern.split(line)) {
                if (curWord.isEmpty()) {
                    continue;
                }
                context.write(Text(curWord.toLowerCase()), one);
            }
        }
    }

    public static class WCReducer
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



}
