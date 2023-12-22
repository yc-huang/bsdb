package ai.bsdb.read.index;

import ai.bsdb.io.AsyncFileReader;
import ai.bsdb.io.NativeFileIO;
import ai.bsdb.io.SimpleAsyncFileReader;
import ai.bsdb.io.UringAsyncFileReader;
import ai.bsdb.read.SyncReader;
import ai.bsdb.read.kv.KVReader;
import ai.bsdb.util.Common;
import ai.bsdb.util.RecyclingBufferPool;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public class AsyncDirectIndexReader extends BaseIndexReader implements AsyncIndexReader {

    private final int fd;
    private final AsyncFileReader asyncFileReader;

    public AsyncDirectIndexReader(File idxFile, int submitThreads, boolean useUring) throws IOException {
        this.size = idxFile.length() / Common.SLOT_SIZE;
        this.fd = NativeFileIO.openForReadDirect(idxFile.toString());

        this.asyncFileReader = useUring ? new UringAsyncFileReader(Common.SLOT_SIZE, submitThreads, "index-reader")
                : new SimpleAsyncFileReader(Common.SLOT_SIZE, submitThreads, "index-reader");
        this.asyncFileReader.start();
    }

    @Override
    public boolean asyncGetAddrAt(long index, KVReader kvReader, byte[] key, CompletionHandler<byte[], Object> appHandler, Object appAttach, SyncReader.IndexCompletionHandler handler) throws InterruptedException {
        //final ByteBuffer buf = bufferPool.get();//ByteBuffer.allocateDirect(Common.SLOT_SIZE);
        long offset = getOffsetFromIndex(index);// & 0xFFFFFFFFFFFFF000L;//offset / 4096 * 4096;
        //int readOffset = 0;//(int)(offset - alignedOffset);
        this.asyncFileReader.read(fd, offset, new CompletionHandler<ByteBuffer, Integer>() {
            @Override
            public void completed(ByteBuffer buf, Integer integer) {
                long addr = buf.getLong();
                //bufferPool.release(buf);
                handler.completed((addr), kvReader, key, appHandler, appAttach);
            }

            @Override
            public void failed(Throwable throwable, Integer o) {
                //bufferPool.release(buf);
                handler.failed(throwable, appHandler, appAttach);
            }
        });
        return false;
    }


}
