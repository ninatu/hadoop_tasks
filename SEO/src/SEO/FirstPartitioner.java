package SEO;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by nina on 21.10.16.
 */
public class FirstPartitioner extends Partitioner<TextPair, NullWritable> {
    @Override
    public int getPartition(TextPair textPair, NullWritable nullWritable, int numPartitions) {
        return (Math.abs(textPair.getFirst().hashCode()) * 127) % numPartitions;
    }
}
