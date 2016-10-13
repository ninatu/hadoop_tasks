package com.nina.technosphere.hadoop.WordCount;

import java.io.IOException;

/**
 * Created by nina on 13.10.16.
 */

public class WCInputFormat
        extends FileInputFormat<LongWritable, Text> {

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        Configuration conf = job.getConf();
        FileSystem fileSystem = new FileSystem(conf);
        Path path = null;
        ContentSummary cSummary = null;
        for (FileStatus status : listStatus(job)) {
            path = status.getPath();
            if (status.isDirctory()) {
                throw new IOException("Not a file: " + fileName);
            }
            cSummary = fileSystem.getContentSummary(path);
            long length = cSummary.getLength();
            splits.add(new FileSplit(path, 0, length, new String[]{}));
        }
    }
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        context.setStatus(genericSplit.toString());
        return new WCRecordReader();
    }
}