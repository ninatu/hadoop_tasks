import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;

public class WebPagesMapper extends TableMapper<Text, Text> {
    protected static final String PARAMETR_WEBPAGES = new String("mapred.table_webpages_name");
    protected static final String PARAMETR_WEBSITES = new String("mapred.table_websites_name");
    private static byte[] webPagesTable;
    private static byte[] webSitesTable;

    private static byte[] cfDocs = Bytes.toBytes("docs");
    private static byte[] columnUrl = Bytes.toBytes("url");
    private static byte[] columnDisabled = Bytes.toBytes("disabled");

    private static byte[] cfInfo = Bytes.toBytes("info");
    private static byte[] columnSite = Bytes.toBytes("site");
    private static byte[] columnRobots = Bytes.toBytes("robots");

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        webPagesTable = Bytes.toBytes(conf.get(PARAMETR_WEBPAGES));
        webSitesTable = Bytes.toBytes(conf.get(PARAMETR_WEBSITES));
    }

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
                    byte[] disabledBytes = columns.getValue(cfDocs, columnDisabled);
                    String disabled = disabledBytes == null ? new String("N") : new String("Y");
                    context.write(new Text("b" + Utils.hostToStandart(host)), new Text(disabled + url));
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

    static public void setWebPagesTable(Job job, String name) {
        job.getConfiguration().set(PARAMETR_WEBPAGES, name);
    }
    static public void setWebSitesTable(Job job, String name) {
        job.getConfiguration().set(PARAMETR_WEBSITES, name);
    }
}
