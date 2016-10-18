package com.nina.technosphere.hadoop.DocCount;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;

/**
 * Created by nina on 14.10.16.
 */

public class DCFileSplit extends FileSplit {
    private long startIndex; // start position in index_file
	private long countDocs;

	public DCFileSplit() {};
    public DCFileSplit(Path _file, long _start, long _length,
                       String[] _hosts, long _startIndex, long _countDocs) {
        super(_file, _start, _length, _hosts);
        startIndex = _startIndex;
		countDocs = 3;//_countDocs;
    }
	
	/*public DCFileSplit(Path file, long start, long length,
                       String[] hosts, long startIndex, long countDocs) {
        super(file, start, length, hosts);
        this.startIndex = startIndex;
		this.countDocs = countDocs;
    }*/

    public long getStartIndex() {
        return startIndex;
    }
	public long getCountDocs() {
		return countDocs;
	}
}
