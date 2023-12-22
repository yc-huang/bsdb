package ai.bsdb.read.kv;

import ai.bsdb.io.NativeFileIO;
import org.apache.commons.configuration2.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public class CompactKVReader extends PartitionedKVReader {
    public CompactKVReader(File kvFile, Configuration config, boolean async, boolean useDirectIO) throws IOException {
        super(kvFile, config, async, useDirectIO, true);
    }

    @Override
    protected ByteBuffer readFromBucket(int partition, long offset) throws IOException {
        if (useDirectIO) {
            ByteBuffer buf = getPooledBuffer();
            NativeFileIO.readAlignedTo4096(this.fds[partition], offset, buf, maxRecordSize);
            return buf;
        } else {
            return this.mmaps[partition].toDirectByteBuffer(offset, maxRecordSize);
        }
    }

    @Override
    protected boolean asyncReadFromBucket(int partition, long offset, CompletionHandler<ByteBuffer, Integer> handler) throws InterruptedException {
        this.asyncReader.read(this.fds[partition], offset, handler);
        return true;
    }
}
