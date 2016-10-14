package com.nina.technosphere.hadoop.DocCount;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Created by nina on 14.10.16.
 */

public class DCFileSplit extends FileSplit {
    private long startIndex; // start position in index_file

    public DCFileSplit(Path file, long start, long length,
                       String[] hosts, long startIndex) {
        super(file, start, length, hosts);
        this.startIndex = startIndex;
    }

    public long getStartIndex() {
        return startIndex;
    }
}
