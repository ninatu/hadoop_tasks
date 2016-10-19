package DocCount;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;


public class DCFileSplit extends FileSplit {
    private long startIndex; // start position in index_file
	private long countDocs;

	public DCFileSplit() {};
    public DCFileSplit(Path _file, long _start, long _length,
                       String[] _hosts, long _startIndex, long _countDocs) {
        super(_file, _start, _length, _hosts);
        startIndex = _startIndex;
		countDocs =_countDocs;
    }
	
    public long getStartIndex() {
        return startIndex;
    }
	public long getCountDocs() {
		return countDocs;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
	    out.writeLong(startIndex);
	    out.writeLong(countDocs);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		startIndex = in.readLong();
		countDocs = in.readLong();
	}
}
