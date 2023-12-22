package ai.bsdb.read.kv;

import ai.bsdb.io.AsyncFileReader;
import ai.bsdb.io.NativeFileIO;
import ai.bsdb.io.SimpleAsyncFileReader;
import ai.bsdb.io.UringAsyncFileReader;
import ai.bsdb.util.Common;
import com.google.common.io.Files;
import org.apache.commons.configuration2.Configuration;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public abstract class PartitionedKVReader extends BaseKVReader {
    protected MMapBuffer[] mmaps;
    protected final int[] fds;
    protected ThreadLocal<ByteBuffer> threadLocalPooledBuffer = new ThreadLocal<>();
    protected AsyncFileReader asyncReader;
    protected final int maxRecordSize;
    protected final boolean useDirectIO;
    protected int partitions;

    public PartitionedKVReader(File kvFile, Configuration config, boolean async, boolean useDirectIO, boolean initReader) throws IOException {
        super(config);
        this.useDirectIO = useDirectIO;
        //maxRecordSize = NativeFileIO.alignToPageSize(config.getInt(Common.CONFIG_KEY_KV_KEY_LEN_MAX) + config.getInt(Common.CONFIG_KEY_KV_VALUE_LEN_MAX) + Common.RECORD_HEADER_SIZE);
        maxRecordSize = config.getInt(Common.CONFIG_KEY_KV_KEY_LEN_MAX) + config.getInt(Common.CONFIG_KEY_KV_VALUE_LEN_MAX) + Common.RECORD_HEADER_SIZE;
        File[] files = listKVFile(kvFile);
        partitions = files.length;
        this.fds = new int[partitions];
        for (File file : files) {
            int partition = getPartitionNum(file.getName());
            this.fds[partition] = NativeFileIO.openForReadDirect(file.toString());
        }

        if (async) {
            if (initReader) {
                startAsyncReader(maxRecordSize);
            }
        } else {
            this.mmaps = new MMapBuffer[partitions];
            for (File file : files) {
                int partition = getPartitionNum(file.getName());
                this.mmaps[partition] = new MMapBuffer(file, MMapMode.READ_ONLY);
            }
        }
    }

    protected void startAsyncReader(int readBlockSize) {
        boolean useUring = Boolean.getBoolean("bsdb.uring");
        int submitThreads = Common.getPropertyAsInt("bsdb.reader.kv.submit.threads", Math.max(Common.CPUS / 2, 2));
        //int callbackThreads = Common.getPropertyAsInt("bsdb.reader.kv.callback.threads", 1);
        this.asyncReader = useUring ?
                new UringAsyncFileReader(readBlockSize, submitThreads, "kv-reader")
                : new SimpleAsyncFileReader(readBlockSize, submitThreads, "kv-reader");
        this.asyncReader.start();
    }

    protected File[] listKVFile(File base) {
        File parent = base.getParentFile();
        String namePrefix = base.getName();
        return parent.listFiles(fileName -> fileName.getName().startsWith(namePrefix));
    }

    protected int getPartitions() {
        return partitions;
    }

    protected int getPartitionNum(String fileName) {
        return Integer.parseInt(Files.getFileExtension(fileName));
    }

    @Override
    protected ByteBuffer readRecord(long addr) throws IOException {
        int bucket = (int) (addr >>> 56);
        long recOffset = addr & 0xFFFFFFFFFFFFFFL;
        return readFromBucket(bucket, recOffset);
    }

    @Override
    protected boolean asyncReadRecord(long addr, CompletionHandler<ByteBuffer, Integer> handler) throws InterruptedException {
        int bucket = (int) (addr >>> 56);
        long recOffset = addr & 0xFFFFFFFFFFFFFFL;
        return asyncReadFromBucket(bucket, recOffset, handler);
    }

    protected ByteBuffer getPooledBuffer() {
        return Common.getBufferFromThreadLocal(threadLocalPooledBuffer, NativeFileIO.getBufferSizeForUnalignedRead(maxRecordSize), true);
    }

    @Override
    protected void returnBuffer(long addr, ByteBuffer buf) {

    }

    protected abstract ByteBuffer readFromBucket(int bucket, long offset) throws IOException;

    protected abstract boolean asyncReadFromBucket(int bucket, long offset, CompletionHandler<ByteBuffer, Integer> handler) throws InterruptedException;

}
