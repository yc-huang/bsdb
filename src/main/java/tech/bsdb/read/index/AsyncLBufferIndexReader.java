package tech.bsdb.read.index;

import tech.bsdb.read.SyncReader;
import tech.bsdb.read.kv.KVReader;
import tech.bsdb.util.Common;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;

import java.io.File;
import java.io.IOException;
import java.nio.channels.CompletionHandler;

public class AsyncLBufferIndexReader extends BaseIndexReader implements AsyncIndexReader {
    LBufferAPI buffer;

    public AsyncLBufferIndexReader(File idxFile) throws IOException {
        this.size = idxFile.length() / Common.SLOT_SIZE;
        MMapBuffer mmap = new MMapBuffer(idxFile, MMapMode.READ_ONLY);
        this.buffer = mmap;
    }

    public long getAddrAt(long index) {
        return Long.reverseBytes(this.buffer.getLong(getOffsetFromIndex(index)));
    }

    @Override
    public boolean asyncGetAddrAt(long index, KVReader kvReader, byte[] key, CompletionHandler<byte[], Object> appHandler, Object appAttach, SyncReader.IndexCompletionHandler handler) {
        handler.completed(getAddrAt(index), kvReader, key, appHandler, appAttach);
        return true;
    }
}
