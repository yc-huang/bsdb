package ai.bsdb.write;

import ai.bsdb.util.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class PartitionedKVWriter extends BaseKVWriter {
    protected final static int DEFAULT_PARTITION_NUM = Runtime.getRuntime().availableProcessors() * 2;
    volatile long count;
    final int partitions;
    private final Lock[] locks;

    Logger logger = LoggerFactory.getLogger(PartitionedKVWriter.class);

    PartitionedKVWriter() {
        this(DEFAULT_PARTITION_NUM);
    }

    PartitionedKVWriter(int partitions) {
        this.partitions = partitions;
        this.locks = new Lock[partitions];
        for (int partition = 0; partition < partitions; partition++) {
            locks[partition] = new ReentrantLock();
        }
    }

    @Override
    public void put0(byte[] key, byte[] value) throws IOException {
        int bucket = choosePartitionAndLock(key);
        //Lock lock = this.locks[bucket];
        //lock.lock();
        try {
            putToPartition(bucket, key, value);
        } finally {
            locks[bucket].unlock();
        }
    }

    public void finish() throws IOException {
        for (int bucket = 0; bucket < partitions; bucket++) flushPartition(bucket);
    }

    @Override
    public long forEach(ScanHandler handler) throws IOException {
        LongAdder scanCount = new LongAdder();
        Common.runParallel(Runtime.getRuntime().availableProcessors() * 2, (pool, futures) -> {
            for (int bucket = 0; bucket < partitions; bucket++) {
                final int currentBucket = bucket;
                futures.add(pool.submit(() -> {
                    try {
                        //System.err.println("start to scan bucket " + finalBucket);
                        scanCount.add(partitionForEach(currentBucket, handler));
                        //System.err.println("finished bucket " + finalBucket);
                    } catch (IOException e) {
                        logger.error("PartitionedKVWriter.forEach encountered error", e);
                        throw new RuntimeException(e);
                    }
                }));
            }
        }, true);//TODO
        long found = scanCount.sum();
        logger.trace("scan found {} records.", found);
        return found;
    }

    abstract void putToPartition(int partition, byte[] key, byte[] value) throws IOException;

    abstract void flushPartition(int partition) throws IOException;

    abstract long partitionForEach(int partition, ScanHandler handler) throws IOException;

    protected File getPartitionFile(File base, int partitionId) {
        return new File(base.getParentFile(), base.getName() + "." + partitionId);
    }

    private int choosePartitionAndLock(byte[] key) {
        int idx = (int) (++count % partitions);
        //search for available lock
        for (int i = idx; i < locks.length; i++) {
            Lock lock = locks[i];
            if (lock.tryLock()) return i;
        }
        for (int i = 0; i < idx; i++) {
            Lock lock = locks[i];
            if (lock.tryLock()) return i;
        }
        //no available, wait...
        locks[idx].lock();
        return idx;
    }
}
