package com.nina.technosphere.hadoop.DocCount;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
/**
 * Created by nina on 13.10.16.
 */

public class DCInputFormat
        extends FileInputFormat<LongWritable, Text> {

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        Configuration conf = job.getConfiguration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = null;
        ContentSummary cSummary = null;
        for (FileStatus status : listStatus(job)) {
            path = status.getPath();
            if (status.isDirectory()) {
                throw new IOException("Not a file: " + path);
            }
            cSummary = fileSystem.getContentSummary(path);
            long length = cSummary.getLength();
            splits.add(new FileSplit(path, 0, length, new String[]{}));
        }
	return splits;
    }
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        context.setStatus(genericSplit.toString());
        return new DCRecordReader();
    }
}
