import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WebPagesReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    String identSite = new String("a");
    String identUrl = new String("b");
    private static byte[] cf = Bytes.toBytes("docs");
    private static byte[] columnDisabled = Bytes.toBytes("disabled");

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        RobotsFilter robots = new RobotsFilter();
        for(Text value: values) {
            String ident = key.toString().substring(0, 1);
            if (ident.compareTo(identSite) == 0) {
                robots.addRules(value.toString());
            } else if (ident.compareTo(identUrl)== 0) {
                String url = value.toString().substring(1);
                String mark = value.toString().substring(0, 1);
                boolean predDisabled = mark.compareTo("Y") == 0 ? true : false;
                boolean rightDisabled = robots.isDisallowed(url);
                if (rightDisabled == true && predDisabled == false) {
                    Put put = new Put(Bytes.toBytes(MD5Hash.digest(url).toString()));
                    put.addColumn(cf, columnDisabled, Bytes.toBytes("Y"));
                    context.write(null, put);
                } else if (rightDisabled == false && predDisabled == true){
                    Delete delete = new Delete(Bytes.toBytes(MD5Hash.digest(url).toString()));
                    delete.addColumn(cf, columnDisabled);
                    context.write(null, delete);
                }
            }
        }
    }
}
