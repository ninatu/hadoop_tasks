import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class WebPagesTest {
    public static void main(String[] args) throws IOException {

        byte[] cfDocs = Bytes.toBytes("docs");
        byte[] columnUrl = Bytes.toBytes("url");
        byte[] columnDisabled = Bytes.toBytes("disabled");

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("webpages_nina"));

        Scan scan = new Scan();
        ResultScanner resultScaner = table.getScanner(scan);
        int i =0;
        for (Result result: resultScaner) {
            PrintResult(result);
            //String url = new String(result.getValue(cfDocs, columnUrl));
            //String disabled = new String(result.getValue(cfDocs, columnDisabled));
            //System.out.println("URL: " + url + " Disabled: " + disabled);
            i++;
            if (i > 10) {

            }

        }


    }
    static void PrintCell(Cell cell) {
        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
        String value = Bytes.toString(CellUtil.cloneValue(cell));
        System.out.printf("\t%s=%s\n", qualifier, value);
    }

    static void PrintResult(Result res) throws UnsupportedEncodingException {
        System.out.printf("------------- ROW: %s\n", new String(res.getRow(), "UTF8"));
        for (Cell cell: res.listCells())
            PrintCell(cell);
    }
}
