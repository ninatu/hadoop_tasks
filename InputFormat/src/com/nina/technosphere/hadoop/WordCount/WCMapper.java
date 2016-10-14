package com.nina.technosphere.hadoop.WordCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configured;
import java.util.regex.Pattern;

/**
 * Created by nina on 13.10.16.
 */

public class WCMapper
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