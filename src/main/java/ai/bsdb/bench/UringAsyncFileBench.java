package ai.bsdb.bench;

import ai.bsdb.io.NativeFileIO;
import ai.bsdb.io.UringAsyncFileReader;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class UringAsyncFileBench {
    static AtomicLong readBytes = new AtomicLong(0);
    static AtomicLong ios = new AtomicLong(0);
    static AtomicLong failed = new AtomicLong(0);
    static AtomicLong submited = new AtomicLong(0);
    static boolean finished = false;
    static long iteration = 50000 * 1000 * 1000L;

    public static void main(String[] args) throws Exception {

        Path path = FileSystems.getDefault().getPath(args[0]);
        int parallel = Integer.parseInt(args[1]);
        int readLen = Integer.parseInt(args[2]);
        int submitThreads = Integer.parseInt(args[3]);
        //int callbackThreads = Integer.parseInt(args[4]);

        long fileLen = path.toFile().length();

        new Thread(() -> {
            while (ios.get() < iteration) {
                long c = ios.get();
                long start = System.currentTimeMillis();
                try {
                    Thread.sleep(1 * 1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.err.printf("submit:%d, complete %d failed %d read bytes %d, iops %d %n", submited.get(), ios.get(), failed.get(), readBytes.get(), (ios.get() - c) * 1000 / (System.currentTimeMillis() - start));
            }

            finished = true;
        }).start();


        for (int i = 0; i < parallel; i++) {
            UringAsyncFileReader reader = new UringAsyncFileReader(readLen, submitThreads, "");
            int fd0 = NativeFileIO.openForReadDirect(path.toString());
            //reader.registerFile(new int[]{fd0});
            int fd = fd0; //0; registered file index is 0
            reader.start();
            new Thread(() -> {
                Random random = new Random();
                CompletionHandler<ByteBuffer, Integer> handler = new CompletionHandler<>() {
                    @Override
                    public void completed(ByteBuffer byteBuffer, Integer len) {
                        ios.getAndIncrement();
                        readBytes.getAndAdd(len);
                    }

                    @Override
                    public void failed(Throwable throwable, Integer integer) {
                        throwable.printStackTrace();
                        failed.getAndIncrement();
                    }
                };
                while (!finished) {
                    long pos = Math.abs(random.nextLong() * 10000004771L) % fileLen;
                    if (pos + readLen >= fileLen) pos -= readLen;
                    pos &= -4096L;

                    try {
                        reader.read(fd, pos, handler);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                submited.getAndIncrement();
            }, "submit-" + i).start();


        }

        while (!finished) {
            Thread.sleep(10000);
            System.gc();
        }
        System.exit(0);
    }
}