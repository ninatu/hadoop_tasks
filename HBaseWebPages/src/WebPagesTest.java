import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class WebPagesTest {
    public static void main(String[] args) throws IOException {

        byte[] cfDocs = Bytes.toBytes("docs");
        byte[] columnUrl = Bytes.toBytes("url");
        byte[] columnDisabled = Bytes.toBytes("disabled");

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("webpages"));

        Scan scan = new Scan();
        ResultScanner resultScaner = table.getScanner(scan);
        for (Result result: resultScaner) {
            String url = new String(result.getValue(cfDocs, columnUrl));
            String disabled = new String(result.getValue(cfDocs, columnDisabled));
            System.out.println("URL: " + url + " Disabled: " + disabled);
            break;
        }


    }
}
