package tech.bsdb.read.index;

import tech.bsdb.util.Common;

public class BaseIndexReader {
    protected long size;
    public long size() {
        return this.size;
    }
    protected long getOffsetFromIndex(long index) {
        return index * Common.SLOT_SIZE;
    }
}
