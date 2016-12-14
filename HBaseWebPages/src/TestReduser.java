import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class TestReduser extends Reducer<Text, Text, Text, Text>{
    String identSite = new String("a");
    String identUrl = new String("b");

    private static byte[] cf = Bytes.toBytes("docs");
    private static byte[] columnDisabled = Bytes.toBytes("disabled");
    MessageDigest rowDigest;
    private Integer index;

    @Override
    public void setup(Context context) {
        index = 0;
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        RobotsFilter robots = new RobotsFilter();
        index += 1;
        for(Text value: values) {
            context.write(new Text(index.toString() + key), value);

            String ident = key.toString().substring(0, 1);
            if (ident.compareTo(identSite) == 0) {
                robots.addRules(value.toString());
                context.write(new Text("addRules"), value);

            } else if (ident.compareTo(identUrl)== 0) {
                String url = value.toString().substring(1);
                String mark = value.toString().substring(0, 1);
                boolean predDisabled = mark.compareTo("Y") == 0 ? true : false;
                boolean rightDisabled = robots.isDisallowed(url);
                if (rightDisabled == true && predDisabled == false) {
                    //Put put = new Put(rowDigest.digest(url.getBytes()));
                    //put.addColumn(cf, columnDisabled, Bytes.toBytes("Y"));
                    context.write(new Text("disable"), value);
                } else if (rightDisabled == false && predDisabled == true){
                    //Delete delete = new Delete(rowDigest.digest(url.getBytes()));
                    //delete.addColumn(cf, columnDisabled);
                    context.write(new Text("notDisable"), value);
                }
            }
        }
    }
}
