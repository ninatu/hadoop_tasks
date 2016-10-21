package SEO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * Created by nina on 21.10.16.
 */
public class SEO extends Configured implements Tool {
    public static int main(String[] args) throws Exception {
        int result = ToolRunner.run(new SEO(), args);
        return result;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static Job getJobConf(Configuration conf, String input, String out_dir) throws IOException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(SEO.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(out_dir));
        // SEOMapper выводит ключ=пара(хост, запрос) и value=null
        job.setMapperClass(SEOMapper.class);
        // FirstPartitioner разделяет по первому полю ключа(по хосту)
        job.setPartitionerClass(FirstPartitioner.class);
        // Сортирует ключи сначала по первому полю потом по второму
        job.setSortComparatorClass(TextPairComparator.class);
        // GroupFirstComparator групирует по первому полю ключа(по хосту)
        job.setGroupingComparatorClass(GroupFirstComparator.class);
        // SEOReduser выводит хост и лучший запрос
        job.setReducerClass(SEOReduser.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }
}
