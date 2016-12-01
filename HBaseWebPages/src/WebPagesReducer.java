import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class WebPagesReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    String identSite = new String("a");
    String identUrl = new String("b");

    private static byte[] cf = Bytes.toBytes("docs");
    private static byte[] columnDisabled = Bytes.toBytes("disabled");
    MessageDigest rowDigest;

    @Override
    public void setup(Context context) {
        try {
            rowDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        RobotsFilter robots = new RobotsFilter();
        for(Text value: values) {
            String ident = key.toString().substring(0, 1);
            if (ident == identSite) {
                robots.addRules(value.toString());
            } else if (ident ==identUrl) {
                String url = value.toString();
                boolean disabled = robots.isDisallowed(url);
                if (disabled) {
                    Put put = new Put(rowDigest.digest(url.getBytes()));
                    put.addColumn(cf, columnDisabled, Bytes.toBytes("Y"));
                    context.write(null, put);
                } else {
                    Delete delete = new Delete(rowDigest.digest(url.getBytes()));
                    delete.addColumn(cf, columnDisabled);
                    context.write(null, delete);
                }
            }
        }
    }
}
