package com.nina.technosphere.hadoop.DocCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.HashSet;
import java.io.IOException;

public class DCMapper
        extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);
    private final static Pattern wordPattern = Pattern.compile("\\p{L}+");
	private final static HashSet<String> setWord = new HashSet<String>();
    
	@Override
    public void map(LongWritable key, Text text, Context context) 
			throws IOException, InterruptedException  {
		setWord.clear();
		Matcher wordMatcher = wordPattern.matcher(text.toString());
		String curWord;
        while (wordMatcher.find()) {
			curWord = wordMatcher.group().toLowerCase();
            if (curWord.isEmpty()) {
                continue;
            }
			setWord.add(curWord);
        }
		for (String word :setWord) {
			context.write(new Text(word), one);
		}
    }
};
