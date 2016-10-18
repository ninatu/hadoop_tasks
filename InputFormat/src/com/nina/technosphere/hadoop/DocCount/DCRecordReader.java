package com.nina.technosphere.hadoop.DocCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.compress.DefaultCodec;
import java.io.ByteArrayInputStream;
import java.lang.String;
import java.io.IOException;
import java.io.EOFException;
import java.lang.Exception;

/**
 * Created by nina on 13.10.16.
 */

public class DCRecordReader extends RecordReader<LongWritable, Text> {
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    byte buffer[] = null;

    private long start;
	private long pos;
    private long end;
    private long startIndex;
	private long countDocs;
	private long curDocs;
    private FSDataInputStream fileStream = null;
    private FSDataInputStream indexStream = null;
    private CompressionInputStream compessStream = null;
    private LongWritable key = null;
    private Text value = null;
    private DefaultCodec codec;
    private Decompressor decompressor;
	
	private TaskAttemptContext myContext;

	enum allDebag {
		CURDOCS,
		CURDOC_CHECK_EOF,
		TRYREAD,
		COUNTDOCSNULL;

	}
    public DCRecordReader() {
    }

    @Override
    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
		myContext = context;
        DCFileSplit split = (DCFileSplit) genericSplit;
        Configuration conf = context.getConfiguration();
        final FileSystem fileSystem = FileSystem.get(conf);
        final Path filePath = split.getPath();
        final Path indexPath = new Path(filePath.toString() + new String(".idx"));

        codec = new DefaultCodec();
        codec.setConf(fileSystem.getConf());
        decompressor = CodecPool.getDecompressor(codec);

        start = split.getStart();
        end = start + split.getLength();
        pos = start;
        startIndex = split.getStartIndex();
		countDocs = split.getCountDocs();
		curDocs = 0;
		if (countDocs == 0)
			myContext.getCounter(allDebag.COUNTDOCSNULL).increment(1);
			
        fileStream = fileSystem.open(filePath);
        indexStream = fileSystem.open(indexPath); 

        fileStream.seek(start);
        indexStream.seek(startIndex);

        buffer = new byte[DEFAULT_BUFFER_SIZE];
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        //If end of file
        //if (fileStream.getPos() >= end) {
        if (curDocs >= countDocs) {
			///!!!!!!!!!!!!!!!!!
			myContext.getCounter(allDebag.CURDOC_CHECK_EOF).increment(1);
			key = null;
            value = null;
			if (indexStream != null) {
				indexStream.close();
				indexStream = null;
			}
			if (fileStream != null) {
				fileStream.close();
				fileStream = null;
			}
            return false;
        }

        //If first item
        if (key == null) {
            key = new LongWritable();
        }
        key.set(fileStream.getPos());
        if (value == null) {
            value = new Text();
        }

        int sizeInput = Integer.reverseBytes(indexStream.readInt());
		curDocs += 1;
		myContext.getCounter(allDebag.CURDOCS).increment(1);
		pos += sizeInput;
        byte inputBytes[] = new byte[sizeInput];

        //int  newSize = 0;
		int  readBytes = 0;
		fileStream.readFully(inputBytes, 0, sizeInput);
		/*
        do {
			//try {
				readBytes = fileStream.read(inputBytes, newSize, sizeInput - newSize);
				if (readBytes <= 0)
					break; // EOF
				newSize += readBytes;
			//} catch (IOException eof) {
			//	break;
			//}
        }  while (newSize != sizeInput);
		*/		
        compessStream = codec.createInputStream(new ByteArrayInputStream(inputBytes), decompressor);
        value.clear();
        while(true) {
			try {
				myContext.getCounter(allDebag.TRYREAD).increment(1);
				readBytes = compessStream.read(buffer);
				if (readBytes <= 0)
					break; // EOF
				value.append(buffer, 0, readBytes);
			} catch (EOFException eof) {
				break;
			}
        }
		compessStream.close();
        return true;
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue()  {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        if (end - start == 0) {
            return 1.0f;
        } else {
            return (float)(pos  - start) / (float) (end - start);
        }
    }

    public synchronized void close() throws IOException {
        if (fileStream != null) {
            fileStream.close();
        }
        if (indexStream != null) {
            indexStream.close();
        }
        if (compessStream != null) {
            IOUtils.closeStream(compessStream); // не уверена...
        }
        CodecPool.returnDecompressor(decompressor);
        buffer = null;
    }
}
