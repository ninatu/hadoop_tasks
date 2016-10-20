package SEO;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by nina on 20.10.16.
 */
public class SEOMapper extends Mapper<LongWritable, Text, TextPair, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] items = line.split("\t");

        if (items.length == 2) {
            try {
                URL url = new URL(items[1]);
                context.write(new TextPair(url.getHost(),items[0]), NullWritable.get());
            } catch (MalformedURLException e) {
                context.getCounter("COMMON_COUNTERS", "MalformedUrls").increment(1);
            }
        } else {
            context.getCounter("COMMON_COUNTERS", "MalformedRecords").increment(1);
        }
    }
}
