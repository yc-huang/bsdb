package ai.bsdb.util;

import ai.bsdb.io.NativeFileIO;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * A pool of buffers which uses a simple reference queue to recycle buffers.
 * <p>
 * Do not use it as generic buffer pool - it is optimized and supports only
 * buffer sizes used by the Zstd classes.
 */
public class RecyclingBufferPool {
    private final int buffSize;
    private final int capacity;
    private final boolean aligned;
    private final AtomicLong size;

    private final AtomicLong borrowed = new AtomicLong();
    private final AtomicLong returned = new AtomicLong();
    private final AtomicLong cleaned = new AtomicLong();
    private final AtomicLong wait = new AtomicLong();

    //private final ConcurrentLinkedQueue<SoftReference<ByteBuffer>> pool;

    private final DisruptorBlockingQueue<SoftReference<ByteBuffer>> pool;

    public RecyclingBufferPool(String name, int capacity, int bufferSize, boolean aligned) {
        this.buffSize = bufferSize;
        this.capacity = capacity;
        this.aligned = aligned;
        //this.pool = new ConcurrentLinkedQueue<SoftReference<ByteBuffer>>();
        this.pool = new DisruptorBlockingQueue<>(capacity);
        this.size = new AtomicLong();

        StatisticsPrinter.addStatistics(new StatisticsPrinter.StatisticsItem() {
            @Override
            public void showStatistics() {
                long b = borrowed.get();
                long r = returned.get();
                System.err.printf("pool:%s, s:%d/%d, b:%d, r:%d, delta:%d, c:%d, wait:%d %n", name, pool.size(), size.get(), b, r, (b - r), cleaned.get(), wait.get());
            }
        });
    }

    public ByteBuffer get() {
        borrowed.getAndIncrement();
        while (true) {
            SoftReference<ByteBuffer> sbuf = null;

            // This if statement introduces a possible race condition of allocating a buffer while we're trying to
            // release one. However, the extra allocation should be considered insignificant in terms of cost.
            // Particularly with respect to throughput.

            sbuf = pool.poll();

            if (sbuf == null) {
                if (size.get() <= capacity) {
                    size.incrementAndGet();
                    return aligned ? NativeFileIO.allocateAlignedBuffer(buffSize) : ByteBuffer.allocateDirect(buffSize);
                } else {
                    wait.incrementAndGet();
                    LockSupport.parkNanos(100);
                }
            } else {
                ByteBuffer buf = sbuf.get();
                if (buf != null) {
                    buf.clear();
                    return buf;
                }
            }
        }
    }


    public void release(ByteBuffer buffer) {
        returned.getAndIncrement();

        if (!pool.add(new SoftReference<>(buffer))) {
            NativeFileIO.freeBuffer(buffer);
            cleaned.getAndIncrement();
        }
    }
}
