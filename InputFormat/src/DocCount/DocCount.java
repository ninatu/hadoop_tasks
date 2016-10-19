package DocCount;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configured;


public class DocCount  extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new DocCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
    
		Job job = Job.getInstance(getConf(), "docCount");
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
		return job.waitForCompletion(true) ? 0 : 1;
    }
}
