package ai.bsdb.read.kv;


import ai.bsdb.io.NativeFileIO;
import ai.bsdb.util.Common;
import ai.bsdb.util.RecyclingBufferPool;
import com.github.luben.zstd.ZstdDecompressCtx;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;

public class CompressedKVReader extends PartitionedKVReader {
    final int BLOCK_HEADER_LEN = 8;
    private final ThreadLocal<ZstdDecompressCtx> decompressor;
    private final ThreadLocal<ByteBuffer> decompressedBlock;
    //private final RecyclingBufferPool[] bufferPoolsForDecompress;
    int maxCompressBlockSize, maxUnCompressBlockSize;
    byte[] sharedDict;
    Logger logger = LoggerFactory.getLogger(CompressedKVReader.class);

    public CompressedKVReader(File baseFile, Configuration config, boolean async, boolean directIO) throws IOException {
        super(baseFile, config, async, directIO, false);
        maxCompressBlockSize = config.getInt(Common.CONFIG_KEY_KV_BLOCK_COMPRESS_LEN_MAX);
        maxUnCompressBlockSize = config.getInt(Common.CONFIG_KEY_KV_BLOCK_LEN_MAX);
        int partitions = getPartitions();

        /*
        this.bufferPoolsForDecompress = new RecyclingBufferPool[partitions];
        for (int partition = 0; partition < partitions; partition++) {
            this.bufferPoolsForDecompress[partition] = new RecyclingBufferPool(this.getClass().getName() + "-decompress", Common.DEFAULT_THREAD_POOL_QUEUE_SIZE, maxUnCompressBlockSize, false);
        }*/

        this.decompressor = new ThreadLocal<>();
        this.decompressedBlock = new ThreadLocal<>();

        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(new File(baseFile.getParentFile(), Common.FILE_NAME_SHARED_DICT).toPath()))) {
            sharedDict = (byte[]) ois.readObject();
        } catch (Exception e) {
            logger.error("fail to load shared zstd dictionary", e);
        }

        if (async) startAsyncReader(maxCompressBlockSize);
    }


    @Override
    protected ByteBuffer readFromBucket(int partition, long offset) throws IOException {
        long blockOffset = (offset >>> 16) & 0xFFFFFFFFFFL;
        int recOffset = (int) (offset & 0xFFFF);

        ByteBuffer bb = null;
        if (useDirectIO) {
            bb = getPooledBuffer();
            NativeFileIO.readAlignedTo4096(this.fds[partition], blockOffset, bb, maxCompressBlockSize);
        } else {
            bb = mmaps[partition].toDirectByteBuffer(blockOffset, maxCompressBlockSize);
        }
        int pos = bb.position();
        short blockLen = bb.getShort();
        short originalLen = bb.getShort();
        ByteBuffer decompressBuf = getThreadLocalPooledDecompressBuffer();

        int len = getDecompressor().decompressDirectByteBuffer(decompressBuf, 0, originalLen, bb, pos + BLOCK_HEADER_LEN, blockLen);

        decompressBuf.limit(len);
        decompressBuf.position(recOffset);


        return decompressBuf;
    }

    @Override
    protected ByteBuffer getPooledBuffer() {
        return Common.getBufferFromThreadLocal(threadLocalPooledBuffer, NativeFileIO.getBufferSizeForUnalignedRead(maxCompressBlockSize), true);
    }

    private ByteBuffer getThreadLocalPooledDecompressBuffer(){
        return Common.getBufferFromThreadLocal(this.decompressedBlock, maxUnCompressBlockSize, false);
    }

    ZstdDecompressCtx getDecompressor() {
        ZstdDecompressCtx ctx = this.decompressor.get();
        if (ctx == null) {
            ctx = new ZstdDecompressCtx();
            ctx.loadDict(sharedDict);
        }
        this.decompressor.set(ctx);
        return ctx;
    }

    @Override
    protected boolean asyncReadFromBucket(int partition, long offset, CompletionHandler<ByteBuffer, Integer> handler) throws InterruptedException {
        long blockOffset = (offset >>> 16) & 0xFFFFFFFFFFL;
        int recOffset = (int) (offset & 0xFFFF);
        this.asyncReader.read(fds[partition], blockOffset, new CompletionHandler<ByteBuffer, Integer>() {
            @Override
            public void completed(ByteBuffer bb, Integer integer) {
                int pos = bb.position();
                short blockLen = bb.getShort();
                short origLen = bb.getShort();
                bb.limit(pos + blockLen + BLOCK_HEADER_LEN);
                //RecyclingBufferPool decompressPool = bufferPoolsForDecompress[partition];
                //ByteBuffer decompressBuf = decompressPool.get();
                ByteBuffer decompressBuf = getThreadLocalPooledDecompressBuffer();
                int len = getDecompressor().decompressDirectByteBuffer(decompressBuf, 0, maxUnCompressBlockSize, bb, pos + BLOCK_HEADER_LEN, blockLen);
                decompressBuf.position(recOffset);
                handler.completed(decompressBuf, len);
            }

            @Override
            public void failed(Throwable throwable, Integer integer) {
                handler.failed(throwable, null);
            }
        });
        return true;
    }
}
