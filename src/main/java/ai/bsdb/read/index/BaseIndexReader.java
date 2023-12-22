package ai.bsdb.read.index;

import ai.bsdb.util.Common;

public class BaseIndexReader {
    protected long size;
    public long size() {
        return this.size;
    }
    protected long getOffsetFromIndex(long index) {
        return index * Common.SLOT_SIZE;
    }
}
