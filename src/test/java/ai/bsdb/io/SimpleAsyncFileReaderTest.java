package ai.bsdb.io;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class SimpleAsyncFileReaderTest extends TestCase {
    String file = "gradlew";
    int fd;

    public void setUp() throws Exception {
        super.setUp();
        fd = NativeFileIO.openForReadDirect(file);
    }

    public void tearDown() throws Exception {
    }

    public void testClose() {
    }

    public void testRead() throws InterruptedException, IOException {
        testRead(1);
        testRead(2);
        testRead(3);
        testRead(8);
        testRead(511);
        testRead(512);
        testRead(513);
        testRead(1000);
        testRead(2000);
        testRead(3000);
        testRead(4000);
        testRead(4095);
        testRead(4096);
        testRead(4097);
        testRead(8192);
        testRead(9000);
        testRead(16000);
    }

    private void testRead(int size) throws InterruptedException, IOException {
        SimpleAsyncFileReader reader = new SimpleAsyncFileReader(size, 1, "");
        reader.start();
        long len = new File(file).length();
        AtomicLong submit = new AtomicLong();
        AtomicLong finished = new AtomicLong();

        for (int i = 0; i < len - size; i++) {
            submit.incrementAndGet();
            reader.read(fd, i, new CompletionHandler<ByteBuffer, Integer>() {
                @Override
                public void completed(ByteBuffer byteBuffer, Integer read) {
                    assertTrue("read size:" + size, read > 0);
                    finished.incrementAndGet();
                }

                @Override
                public void failed(Throwable throwable, Integer integer) {
                    throwable.printStackTrace();
                    fail();
                    finished.incrementAndGet();
                }
            });
        }

        while (finished.get() < submit.get()) {
            //System.err.println( submit.get() + " -> " + finished.get());
            LockSupport.parkNanos(100000000);
        }
        reader.close();
    }
}