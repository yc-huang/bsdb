package tech.bsdb.io;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NativeFileIOTest extends TestCase {
    String file = "gradlew";
    int readSize = 512;
    int fd;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        fd = NativeFileIO.openForReadDirect(file);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        NativeFileIO.close(fd);
    }

    public void testOpenForReadDirect() throws IOException {
        int fd = NativeFileIO.openForReadDirect(file);
        assertTrue(fd > 0);
        NativeFileIO.close(fd);
    }

    public void testOpen() {

    }

    public void testClose() {
    }

    public void testAllocateAlignedBuffer() {

    }

    public void testFreeBuffer() {
        ByteBuffer buf = NativeFileIO.allocateAlignedBuffer(4096);
        NativeFileIO.freeBuffer(buf);
    }

    public void testReadAlignedTo512() throws IOException {
        ByteBuffer buf = NativeFileIO.allocateAlignedBuffer(4096 + 512);
        for (int pos = 0; pos < 4100; pos++) {
            buf.clear();
            long readLen = NativeFileIO.readAlignedTo512(fd, pos, buf, readSize);
            assertTrue(readLen > 0);
            assertEquals(pos % 4096, buf.position());//position aligned to 4096
            assertTrue(buf.remaining() >= readSize);
        }
        NativeFileIO.freeBuffer(buf);
    }

    public void testReadAlignedTo4096() throws IOException {
        ByteBuffer buf = NativeFileIO.allocateAlignedBuffer(4096 + 4096);
        for (int pos = 0; pos < 4100; pos++) {
            buf.clear();
            long readLen = NativeFileIO.readAlignedTo4096(fd, pos, buf, readSize);
            assertTrue(readLen > 0);
            assertEquals(pos % 4096, buf.position());
            assertTrue(buf.remaining() >= readSize);
        }
        NativeFileIO.freeBuffer(buf);
    }
}