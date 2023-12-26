package tech.bsdb.read.index;

import tech.bsdb.io.NativeFileIO;
import tech.bsdb.util.Common;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DirectIndexReader extends BaseIndexReader implements IndexReader {
    private final long size;
    private final int fd;
    ThreadLocal<ByteBuffer> threadLocalPooledBuffer;

    public DirectIndexReader(File idxFile) throws IOException {
        this.size = idxFile.length() / Common.SLOT_SIZE;
        this.fd = NativeFileIO.openForReadDirect(idxFile.toString());
        threadLocalPooledBuffer = new ThreadLocal<>();
    }

    public long size() {
        return this.size;
    }

    public long getAddrAt(long index) throws Exception {
        ByteBuffer buf = getPooledBuffer();
        read(index, buf);
        return buf.getLong();
    }

    @Override
    public byte[] getRawBytesAt(long index) throws IOException {
        ByteBuffer buf = getPooledBuffer();
            read(index, buf);
            byte[] ret = new byte[Common.SLOT_SIZE];
            buf.get(ret);
            return ret;
    }

    private ByteBuffer getPooledBuffer(){
        return Common.getBufferFromThreadLocal(threadLocalPooledBuffer, NativeFileIO.getBufferSizeForUnalignedRead(Common.SLOT_SIZE), true);
    }

    private void read(long index, ByteBuffer buf) throws IOException {
        NativeFileIO.readAlignedTo4096(fd, getOffsetFromIndex(index), buf, Common.SLOT_SIZE);
    }
}
