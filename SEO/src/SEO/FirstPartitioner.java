package SEO;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by nina on 21.10.16.
 */
public class FirstPartitioner extends Partitioner<TextPair, IntWritable> {
    @Override
    public int getPartition(TextPair textPair, IntWritable value, int numPartitions) {
        return Math.abs(textPair.getFirst().hashCode() * 127) % numPartitions;
    }
}
