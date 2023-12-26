package tech.bsdb.read.kv;

import tech.bsdb.io.NativeFileIO;
import org.apache.commons.configuration2.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public class BlockedKVReader extends PartitionedKVReader {
    public BlockedKVReader(File kvFile, Configuration config, boolean async, boolean useDirectIO) throws IOException {
        super(kvFile, config, async, useDirectIO, true);
    }

    @Override
    protected ByteBuffer readFromBucket(int partition, long offset) throws IOException {
        long blockOffset = getBlockOffset(offset);
        int blockSize = getBlockSize(offset);
        int recordOffset = getRecordOffset(offset);
        ByteBuffer buf;
        if (useDirectIO) {
            buf = getPooledBuffer();
            NativeFileIO.readAlignedTo4096(this.fds[partition], blockOffset, buf, blockSize);
        } else {
            buf = this.mmaps[partition].toDirectByteBuffer(blockOffset, blockSize);
        }
        buf.position(recordOffset);
        return buf;
    }

    @Override
    protected boolean asyncReadFromBucket(int partition, long offset, CompletionHandler<ByteBuffer, Integer> handler) throws InterruptedException {
        long blockOffset = getBlockOffset(offset);
        int blockSize = getBlockSize(offset);
        int recordOffset = getRecordOffset(offset);
        int estimatedRecordSize = Math.min(maxRecordSize, blockSize - recordOffset);//records in blocked database should not cross blocks/large-blocks
        this.asyncReader.read(this.fds[partition], blockOffset + recordOffset, estimatedRecordSize, handler);
        return true;
    }

    long getBlockOffset(long addr) {
        return ((addr >>> 16) & 0xFFFFFFFFL) * NativeFileIO.PAGE_SIZE;
    }

    int getBlockSize(long addr) {
        return (int) ((addr >>> 48) & 0xFFL) * NativeFileIO.PAGE_SIZE;
    }

    int getRecordOffset(long addr) {
        return (int) (addr & 0xFFFFL);
    }

}
