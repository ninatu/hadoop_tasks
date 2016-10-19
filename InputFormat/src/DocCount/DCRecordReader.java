package DocCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.compress.DefaultCodec;
import java.io.ByteArrayInputStream;
import java.lang.String;
import java.io.IOException;
import java.io.EOFException;


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
	
    public DCRecordReader() {
    }

    @Override
    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
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
			
        fileStream = fileSystem.open(filePath);
        indexStream = fileSystem.open(indexPath); 

        fileStream.seek(start);
        indexStream.seek(startIndex);
        buffer = new byte[DEFAULT_BUFFER_SIZE];
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        //If end of file
        if (curDocs >= countDocs) {
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
		pos += sizeInput;
        byte inputBytes[] = new byte[sizeInput];
		fileStream.readFully(inputBytes, 0, sizeInput);
        compessStream = codec.createInputStream(new ByteArrayInputStream(inputBytes), decompressor);
    
		value.clear();
		int  readBytes = 0;
        while(true) {
			try {
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
        CodecPool.returnDecompressor(decompressor);
        buffer = null;
    }
}
