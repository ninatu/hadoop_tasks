package com.nina.technosphere.hadoop.WordCount;

/**
 * Created by nina on 13.10.16.
 */
public class WordCount extends Configured implements Tool {

    public static void main(String[] args) {
        int res = ToolRunner.run(new WordCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "wordcount");
        job.setJarByClass(this.getClass());

        job.setInputFormatClass(WCInputFormat.class);
        job.setMapperClass(WCMapper.class);
        job.setCombinerClass(WCReducer.class);
        job.setReduserClass(WCReducer.class);
        job.setOuputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}