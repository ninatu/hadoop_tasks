package com.nina.technosphere.hadoop.WordCount;

/**
 * Created by nina on 13.10.16.
 */

public class WCRecordReader extends RecordReader<LongWritable, Text> {
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    byte buffer[DEFAULT_BUFFER_SIZE];

    private long start;
    private long pos;
    private long end;
    private FSDataInputStream fileStream = null;
    private FSDataInputStream indexStream = null;
    private CompressionInputStream compessStream = null;
    private LongWritable key = null;
    private Text value = null;
    private CompressionCodec codec;
    private Decompressor decompressor;

    public WCRecordReader() {
    }

    @Override
    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration conf = context.getConfiguration();
        final FileSystem fileSystem = FileSystem.get(conf);
        final Path filePath = split.getPath();
        final Path indexPath = new Path(filePath.toString() + String(".idx"));

        codec = new DefaultCodec();
        codec.setConf(fileSystem.getConf());
        decompressor = CodecPool.getDecompressor(codec)
        //decompressor = codec.createDecompressor();

        fileStream = fileSystem.open(filePath);
        indexStream = fileSystem.open(indexPath); // проверить на  правильность открытия
        start = split.getStart();
        end = start + split.getLength();
        pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        //If end of file
        if (pos == end) {
            key = null;
            value = null;
            return false;
        }

        //If first item
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }

        long sizeInput = Integer.reverseBytes(indexStream.getInt());
        byte inputBytes[] = new byte[size_input];

        int newSize = 0;
        int readBytes = 0;
        do {
            readBytes = fileStream.read(inputBytes, newSize, sizeInput - newSize));
            if (readBytes <= 0)
                break; // EOF
            newSize += readBytes;
        }  while (newSize != sizeInput);

        compessStream = codec.createInputStream(new ByteArrayInputStream(inputBytes), decompressor);
        /// где его закрывать???
        value.clear();
        while(true) {
            readBytes = compessStream.read(buffer);
            if (readBytes <= 0)
                break; // EOF
            value.append(readBytes, 0, readBytes);
            pos += readBytes;
        }
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
            return (float) (pos - start) / (float) (end - start);
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
            IOUtil.closeStream(compessStream); // не уверена...
        }
        CodecPool.returnDecompressor(decompressor);
    }
}
