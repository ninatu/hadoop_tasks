import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;

public class WebPagesMapper extends TableMapper<Text, Text> {
    private static byte[] webPagesTable = Bytes.toBytes("webpages_nina");
    private static byte[] webSitesTable = Bytes.toBytes("websites_nina");

    private static byte[] cfDocs = Bytes.toBytes("docs");
    private static byte[] columnUrl = Bytes.toBytes("url");
    private static byte[] columnDisabled = Bytes.toBytes("disabled");

    private static byte[] cfInfo = Bytes.toBytes("info");
    private static byte[] columnSite = Bytes.toBytes("site");
    private static byte[] columnRobots = Bytes.toBytes("robots");

    @Override
    public void map(ImmutableBytesWritable rowKey, Result columns, Context context) throws IOException, InterruptedException {
        TableSplit currentSplit = (TableSplit)context.getInputSplit();
        byte[] tableName = currentSplit.getTableName();

        if (Arrays.equals(tableName, webPagesTable)) {
            String url = new String(columns.getValue(cfDocs, columnUrl));
            if (url != null) {
                try {
                    URL t_url = new URL(url);
                    String host = t_url.getHost();
                    context.write(new Text("b" + Utils.hostToStandart(host)), new Text(url));
                } catch (MalformedURLException e) {

                }
            }
        } else if (Arrays.equals(tableName, webSitesTable)) {
            byte[] siteBytes = columns.getValue(cfInfo, columnSite);
            byte[] robotsBytes = columns.getValue(cfInfo, columnRobots);
            if (robotsBytes != null && siteBytes != null) {
                context.write(new Text("a" + Utils.hostToStandart(new String(siteBytes))),
                        new Text(new String(robotsBytes)));
            }
        }

    }
}
