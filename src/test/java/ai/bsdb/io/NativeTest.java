package ai.bsdb.io;

import junit.framework.TestCase;

import java.io.IOException;

public class NativeTest extends TestCase {
    String file = "build.gradle";

    public void testInitUring() throws IOException {
    }

    public void testExitUring() {
    }

    public void testRegisterBuffers() {
    }

    public void testUnregisterBuffers() {
    }

    public void testRegisterFiles() {
    }

    public void testUnregisterFiles() {
    }

    public void testAvailableSQ() throws IOException {
        try(Native.Uring uring = new Native.Uring(128, 0)) {
            int available = uring.availableSQ();
            assertTrue(available > 0);
        }
    }

    public void testPrepareRead() {
    }

    public void testPrepareReadM() {
    }

    public void testPrepareReads() {
    }

    public void testPrepareReadFixed() {
    }

    public void testPrepareWrite() {
    }

    public void testPrepareWrites() {
    }

    public void testPrepareWriteFixed() {
    }

    public void testPrepareFsync() {
    }

    public void testSubmit() throws IOException {
    }

    public void testSubmitAndWait() {
    }

    public void testWaitCQEntryTimeout() {
    }

    public void testWaitCQEntries() {
    }

    public void testPeekCQEntries() {
    }

    public void testAdvanceCQ() {
    }

    public void testOpenForRead() {
        int fd = Native.open(file, Native.O_RDONLY);
        assertTrue(fd >= 0);
        Native.close(fd);
    }

    public void testOpenForReadDirect() {
        int fd = Native.open(file, Native.O_RDONLY | Native.O_DIRECT);
        assertTrue(fd >= 0);

        long buf = Native.allocateAligned(512, 4096);
        assertTrue(buf > 0);
        long read = Native.pread(fd, 0, buf, 512);
        assertTrue(read > 0);
        Native.free(buf);
        Native.close(fd);
    }

    public void testAllocateAligned() {
        long addr = Native.allocateAligned(512, 4096);
        assertEquals(0, addr % 4096);
        Native.free(addr);
    }

    public void testFree() {
    }

    public void testPread() {
    }

    public void testClose() {
    }
}