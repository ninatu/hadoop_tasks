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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
/**
 * Created by nina on 13.10.16.
 */

public class DCInputFormat
        extends FileInputFormat<LongWritable, Text> {
    public static final String BYTES_PER_MAP = "mapreduce.input.indexedgz.bytespermap";

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        int numBytesPerSplit = getNumBytesPerSplit(job);
        for (FileStatus status : listStatus(job)) {
            splits.addAll(getSplitsForFile(status, job.getConfiguration(), numBytesPerSplit));
        }
	    return splits;
    }


    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        context.setStatus(genericSplit.toString());
        return new DCRecordReader();
    }


    public static List<DCFileSplit> getSplitsForFile(
            FileStatus status, Configuration conf, int numBytesPerSplit) throws IOException {
        List<DCFileSplit> splits = new ArrayList<DCFileSplit>();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = path = status.getPath();
        if (status.isDirectory()) {
            throw new IOException("Not a file: " + path);
        }
        FileSystem cSummary = fileSystem.getContentSummary(path);
        long all_length = cSummary.getLength();

        Path indexPath = new Path(filePath.toString() + new String(".idx"));
        if (fileSystem.exists(indexPath) == false) {
            throw new IOException("Don't exists a file: " + indexPath);
        }
                if (fileSystem.isFile(indexPath) == false) {
            throw new IOException("Not a file: " + indexPath);
        }

        FSDataInputStream indexStream  = fileSystem.open(indexPath);

        int beginFile = 0;
        int beginIndex = 0;
        int lengthFile = 0;
        int lengthIndex = 0
        while(true) {
            try {
                int sizeDoc = Integer.reverseBytes(indexStream.readInt());
                lengthFile += sizeDoc;
                lengthIndex += 4;
                if (lengthFile >= numBytesPerSplit) {
                    splits.add(DCFileSplit(path, beginFile, lengthFile,
                            new String[]{}, beginIndex));
                    beginFile += lengthFile;
                    beginIndex += lengthIndex;
                    lengthFile = 0;
                    lengthIndex = 0;

                }

            } catch (EOFException eof) {
                if (lengthFile != 0) {
                    splits.add(DCFileSplit(path, beginFile, lengthFile,
                            new String[]{}, beginIndex));
                }
                break;
            }
        }
        return splits;
    }

    public static void setNumBytesPerSplit(Job job, int numLines) {
        job.getConfiguration().setInt(BYTES_PER_MAP, numLines);
    }
    public static int getNumBytesPerSplit(JobContext job) {
        return job.getConfiguration().getInt(BYTES_PER_MAP, 1);
    }

}
