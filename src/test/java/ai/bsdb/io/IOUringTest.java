package ai.bsdb.io;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class IOUringTest extends TestCase {
    IOUring ring;
    int fd;
    ByteBuffer buf;

    public void setUp() throws Exception {
        super.setUp();

        int readLen = 36;
for(long i = 0; i < 81920; i++) {
    if(i % 4096 + readLen > 4096) continue;
    long readPosition = i & -4096L;//align to 4096
    long offset = (int) (i - readPosition);
    //System.err.println("offset:" + offset);

    int limit = (int) (offset + readLen);
    long alignedReadSize = NativeFileIO.alignToPageSize(limit);
    if(alignedReadSize > 4096) System.err.printf("position:%d, aligned size:%d %n", i, alignedReadSize);
}
        ring = new IOUring(128, 0);
        fd = NativeFileIO.openForReadDirect("./build.gradle");
        buf = NativeFileIO.allocateAlignedBuffer(512);
    }

    public void tearDown() throws Exception {
        ring.shutdown();
        NativeFileIO.close(fd);
    }

    public void testShutdown() {
    }

    public void testAvailableSQ() {
        assertTrue(ring.availableSQ() > 0);
    }

    public void testPrepareRead() {
        ring.prepareRead(0, fd, 0, buf, 512);
        int s = ring.submit();
        assertEquals(1, s);
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        IOUring.IOResult result = ring.waitCQEntry();
        assertNotNull(result);
        assertEquals(0, result.reqId);
        assertEquals(512, result.res);
        ring.seenCQEntry(1);
    }

    public void testTestPrepareRead() {



    }

    public void testSubmit() {
    }

    public void testWaitCQEntry() {
    }

    public void testWaitCQEntries() {
    }

    public void testSeenCQEntry() {
    }

    public void testPeekCQEntries() {
    }

    public void testPeekCQEntriesNoop() {
        ring.prepareRead(0, fd, 0, buf, 512);
        ring.prepareRead(1, fd, 512, buf, 512);
        int s = ring.submit();
        assertEquals(2, s);
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        long[][] result = ring.peekCQEntriesNoop(10);
        assertNotNull(result);
        assertEquals(2, result[0].length);
        assertTrue(result[0][0] == 0 | result[0][0] == 1);
        assertEquals(512, result[1][0]);
        assertTrue(result[0][1] == 0 | result[0][1] == 1);
        assertEquals(512, result[1][1]);
        ring.seenCQEntry(2);
    }

    public void testTestPeekCQEntriesNoop() {
    }

    public void testWaitCQEntryTimeout() {
        IOUring.IOResult result = ring.waitCQEntryTimeout(100);
        assertNull(result);
        ring.prepareRead(0, fd, 0, buf, 512);
        int s = ring.submit();
        assertEquals(1, s);
        result = ring.waitCQEntryTimeout(100);
        assertNotNull(result);
        assertEquals(0, result.reqId);
        assertEquals(512, result.res);
        ring.seenCQEntry(1);
    }
}