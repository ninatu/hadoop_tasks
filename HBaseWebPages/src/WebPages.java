import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WebPages extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(HBaseConfiguration.create(), new WebPages(), args);
        System.exit(rc);
    }

    Job getJobConf(String webpages_table, String websites_table) throws IOException {
        Job job = Job.getInstance(getConf(), "HBaseWebPages");
        job.setJarByClass(WebPages.class);

        /// инициализируем scans
        List<Scan> scans = new ArrayList<>();
        Scan scan1 = new Scan();
        Scan scan2 = new Scan();
        scan1.setAttribute("scan.attributes.table.name", Bytes.toBytes(webpages_table));
        scan2.setAttribute("scan.attributes.table.name", Bytes.toBytes(websites_table));
        scans.add(scan1);
        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(scans, WebPagesMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(webpages_table, WebPagesReducer.class, job);
        job.setGroupingComparatorClass(WebPagesGroupComporator.class);
        job.setPartitionerClass(WebPagesPartitioner.class);
        job.setNumReduceTasks(2);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
