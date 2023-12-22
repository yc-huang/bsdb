package ai.bsdb.io;

import ai.bsdb.util.StatisticsPrinter;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public abstract class BaseAsyncFileReader implements AsyncFileReader {
    BlockingQueue<ReadOperation> submitQueue;
    //BlockingQueue<ReadOperation> callbackQueue;
    int maxReadLen;
    boolean running = true;
    //AtomicLong reqIdGen = new AtomicLong();
    int timeout = 1000;//in nano seconds
    int submitThreads, callbackThreads;
    private final ByteBuffer[][] buffers;
    private final String tag;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public BaseAsyncFileReader(int maxReadLen, int submitThreads, int callbackThreads, String tag) {
        this.maxReadLen = maxReadLen;

        this.submitQueue = new DisruptorBlockingQueue<>(QD * 8 * submitThreads, SpinPolicy.BLOCKING);//new ArrayBlockingQueue<>(QD * 8 * submitThreads);//new ArrayBlockingQueue<>(QD * submitThreads);
        //this.callbackQueue = new ArrayBlockingQueue<>(QD * 8 * submitThreads);//new DisruptorBlockingQueueD<>(QD * submitThreads, SpinPolicy.WAITING);
        this.submitThreads = submitThreads;
        this.callbackThreads = callbackThreads;
        this.tag = tag;

        buffers = new ByteBuffer[submitThreads][QD];
        int bufferLen = NativeFileIO.getBufferSizeForUnalignedRead(maxReadLen);
        for (int p = 0; p < submitThreads; p++) {
            buffers[p] = new ByteBuffer[QD];
            for (int i = 0; i < QD; i++) {
                buffers[p][i] = NativeFileIO.allocateAlignedBuffer(bufferLen);
            }
        }

        StatisticsPrinter.addStatistics(() -> System.err.printf("submit queue-%s free/used %d/%d %n", tag, submitQueue.remainingCapacity(), submitQueue.size()));
    }

    @Override
    public void read(int fd, long position, CompletionHandler<ByteBuffer, Integer> handler) throws InterruptedException {
        read(fd, position, maxReadLen, handler);
    }

    @Override
    public void read(int fd, long position, int readLen, CompletionHandler<ByteBuffer, Integer> handler) throws InterruptedException {
        ReadOperation op = new ReadOperation();
        //op.reqId = reqIdGen.incrementAndGet();
        op.fd = fd;
        op.readPosition = position & -4096L;//align to 4096
        op.handler = handler;
        op.offset = (int) (position - op.readPosition);
        op.limit = op.offset + readLen;
        alignedOpReadSize(op);

        submitQueue.put(op);
    }

    @Override
    public void start() {
        if (!started.getAndSet(true)) {
            running = true;
            //RecyclingBufferPool pool = new RecyclingBufferPool(BaseAsyncFileReader.class.getName() + "-" + tag, QD * 32 * submitThreads, bufferLen, true);

            for (int t = 0; t < submitThreads; t++) {

                int finalT = t;
                new Thread(() -> {
                    ReadOperation[] reqs = new ReadOperation[QD];
                    //RecyclingBufferPool pool = new RecyclingBufferPool(BaseAsyncFileReader.class.getName() + "-" + tag + "-" + finalT,QD * 64, bufferLen, true);

                    while (running) {
                        //boolean respProgress = pollResponseAndCallback(finalT, resultMap);

                        ReadOperation op;
                        int current = 0;

                        while ((op = submitQueue.poll()) != null) {
                            //op.readBuffer = pool.get();
                            //op.attach = pool;
                            reqs[current] = op;
                            if (++current == QD) {
                                break;
                            }
                        }

                        if (current > 0) {
                            try {
                                submitRequest(finalT, reqs, current);
                            } catch (IOException | InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            timeout = DEFAULT_TIMEOUT;
                        }else{
                            LockSupport.parkNanos(timeout);
                            timeout = Math.min(MAX_TIMEOUT, timeout + DEFAULT_TIMEOUT);
                        }


                    }
                }, "UringAsyncFileReader-" + tag + "-poll-thread-" + t).start();

            }
            int bs = Math.max(1, (int) Math.ceil(submitThreads * 1.0 / callbackThreads));
            for (int t = 0; t < callbackThreads; t++) {
                int start = bs * t;
                int end = Math.min(start + bs, submitThreads);
                new Thread(() -> {
                    while (running) {
                        boolean progress = false;
                        for (int i = start; i < end; i++) progress = pollResponseAndCallback(i);
                        if (progress) {
                            timeout = DEFAULT_TIMEOUT;
                        } else {
                            LockSupport.parkNanos(timeout);
                            timeout = Math.min(MAX_TIMEOUT, timeout + DEFAULT_TIMEOUT);
                        }
                     /*try {
                        ReadOperation op = callbackQueue.poll(1000, TimeUnit.MILLISECONDS);
                        if (op != null) {
                            ByteBuffer buf = op.readBuffer;
                            try {
                                int bytesRead = op.readResponse;
                                if (bytesRead >= 0) {
                                    //buf.limit(Math.min(op.limit, bytesRead));
                                    buf.position(op.offset);
                                    op.handler.completed(buf, bytesRead);
                                } else {
                                    op.handler.failed(new IOException("read return " + bytesRead + ", pos:" + op.readPosition + ", len:" + bufferLen), bytesRead);
                                }
                            } finally {
                                ((RecyclingBufferPool) op.attach).release(buf);
                            }
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }*/
                    }
                }, "UringAsyncFileReader-" + tag + "-callback-thread-" + t).start();
            }
        }
    }


    public void close() throws IOException {
        this.running = false;
        close0();
    }

    protected void callback(ReadOperation op, int bytesRead) {
        if (op != null) {
            ByteBuffer buf = op.readBuffer;
            try {
                if (bytesRead >= 0) {
                    //buf.limit(Math.min(op.limit, bytesRead));
                    buf.position(op.offset);
                    op.handler.completed(buf, bytesRead);
                } else {
                    op.handler.failed(new IOException("read return " + bytesRead + ", pos:" + op.readPosition + ", len:" + op.alignedReadSize), bytesRead);
                }
            } finally {
                // ((RecyclingBufferPool) op.attach).release(buf);
            }
        }
    }

    private void alignedOpReadSize(ReadOperation op) {
        op.alignedReadSize = NativeFileIO.alignToPageSize(op.limit);
    }
    protected ByteBuffer[] getPartitionPooledBuffer(int partition){
        return buffers[partition];
    }

    abstract void submitRequest(int partition, ReadOperation[] reqs, int num) throws IOException, InterruptedException;
    abstract boolean pollResponseAndCallback(int partition);
    abstract void close0() throws IOException;
}
