package com.nina.technosphere.hadoop.DocCount;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
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
import java.io.EOFException;
import java.util.List;
import java.util.ArrayList;
/**
 * Created by nina on 13.10.16.
 */

public class DCInputFormat
        extends FileInputFormat<LongWritable, Text> {
    public static final String BYTES_PER_MAP = "mapreduce.input.indexedgz.bytespermap";
    
	public List<InputSplit> getSplits (JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        long numBytesPerSplit = getNumBytesPerSplit(job);
        for (FileStatus status : listStatus(job)) {
            splits.addAll(getSplitsForFile(status, job.getConfiguration(), numBytesPerSplit));
        }
	    return splits;
    }

    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        //context.setStatus(genericSplit.toString());
		DCRecordReader recordReader = new DCRecordReader();
		//recordReader.initialize(genericSplit, context);
        return recordReader;
    }

    public List<DCFileSplit> getSplitsForFile(
            FileStatus status, Configuration conf, long numBytesPerSplit) throws IOException {
        List<DCFileSplit> splits = new ArrayList<DCFileSplit>();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = status.getPath();
        if (status.isDirectory()) {
            throw new IOException("Not a file: " + path);
        }

        Path indexPath = new Path(path.toString() + new String(".idx"));
        if (fileSystem.exists(indexPath) == false) {
            throw new IOException("Don't exists a file: " + indexPath);
        }
        if (fileSystem.isFile(indexPath) == false) {
            throw new IOException("Not a file: " + indexPath);
        }
		FSDataInputStream indexStream = null;
		try {

			indexStream  = fileSystem.open(indexPath);

			long beginFile = 0;
			long beginIndex = 0;
			long lengthFile = 0;
			long lengthIndex = 0;
			long countDocs = 0;
			while(true) {
				try {
					int sizeDoc = Integer.reverseBytes(indexStream.readInt());
					lengthFile += sizeDoc;
					lengthIndex += 4;
					countDocs += 1;
					if (lengthFile >= numBytesPerSplit) {
						splits.add(new DCFileSplit(path, beginFile, lengthFile, new String[]{}, beginIndex, countDocs));
						beginFile += lengthFile;
						beginIndex += lengthIndex;
						lengthFile = 0;
						lengthIndex = 0;
						countDocs = 0;
					}
				} catch (EOFException eof) {
					if (countDocs != 0) {
						splits.add(new DCFileSplit(path, beginFile, lengthFile, new String[]{}, beginIndex, countDocs));
					}
					break;
				}
			}
		} finally  {
			if (indexStream != null) {
				indexStream.close();
			}
		}
					
        return splits;
    }

    public static void setNumBytesPerSplit(Job job, long  numLines) {
        job.getConfiguration().setLong(BYTES_PER_MAP, numLines);
    }
    public static long getNumBytesPerSplit(JobContext job) {
        return job.getConfiguration().getLong(BYTES_PER_MAP, 20000000);
    }

}
