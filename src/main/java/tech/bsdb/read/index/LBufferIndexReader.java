package tech.bsdb.read.index;

import tech.bsdb.util.Common;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;

import java.io.File;
import java.io.IOException;

public class LBufferIndexReader extends BaseIndexReader implements IndexReader {
    LBufferAPI buffer;

    public LBufferIndexReader(File idxFile, boolean loadToMemory) throws IOException {
        this.size = idxFile.length() / Common.SLOT_SIZE;
        MMapBuffer mmap = new MMapBuffer(idxFile, MMapMode.READ_ONLY);
        if (loadToMemory) {
            this.buffer = new LBuffer(mmap.size());
            mmap.copyTo(0, this.buffer, 0, mmap.size());
            //mmap.release();
            mmap.close();
        } else {
            this.buffer = mmap;
        }
    }

    public long getAddrAt(long index) {
        return Long.reverseBytes(this.buffer.getLong(getOffsetFromIndex(index)));
    }

    @Override
    public byte[] getRawBytesAt(long index) {
        byte[] buf = new byte[8];
        Common.copyTo(this.buffer, index * Common.SLOT_SIZE, buf, 0, buf.length);
        return buf;
    }
}
