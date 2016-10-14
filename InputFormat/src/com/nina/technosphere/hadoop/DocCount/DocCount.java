package com.nina.technosphere.hadoop.DocCount;

import java.io.IOException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 * Created by nina on 13.10.16.
 */

public class DocCount {
	//private static final Logger LOG = Logger.getLogger(DocCount.class);

    /*public static void main(String[] args) {
        int res = ToolRunner.run(new DocCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
    */
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "doccount");
        job.setJarByClass(DocCount.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(DCInputFormat.class);
        job.setMapperClass(DCMapper.class);
        job.setCombinerClass(DCReducer.class);
        job.setReducerClass(DCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
