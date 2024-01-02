package tech.bsdb.io;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        try (Native.Uring uring = new Native.Uring(128, 0)) {
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

    public void testLoadHash() throws IOException {
        int ITERATIONS = 1000000;
        String hashFileRaw = "./gov_min_hash.raw";
        String hashFileObj = "./gov_min_hash.obj";
        List<byte[]> keys = new ArrayList<byte[]>();
        for (int i = 0; i < ITERATIONS; i++) {
            keys.add(("" + i).getBytes());
        }
        GOVMinimalPerfectHashFunction.Builder<byte[]> builder = new GOVMinimalPerfectHashFunction.Builder<byte[]>();
        builder.keys(keys).transform(TransformationStrategies.byteArray()).signed(12);
        GOVMinimalPerfectHashFunction<byte[]> hashFunction = builder.build();
        hashFunction.dump(hashFileRaw);
        BinIO.storeObject(hashFunction, hashFileObj);

        long hashFun = Native.loadHash(hashFileRaw);
        for (int i = 0; i < ITERATIONS; i++) {
            byte[] key = ("" + i).getBytes();
            long hash1 = hashFunction.getLong(key);
            long hash2 = Native.getHash(hashFun, key);
            assertEquals(hash1, hash2);
        }

        long start = System.nanoTime();
        long hash = 0;
        for (int i = 0; i < 1000; i++) {
            for (byte[] key : keys) {
                hash += hashFunction.getLong(key);
            }
        }
        long cost = System.nanoTime() - start;
        System.err.printf("hash using java cost:  %d %d %n", cost, cost / ITERATIONS / 1000);

        start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            for (byte[] key : keys) {
                hash += Native.getHash(hashFun, key);
            }
        }
        cost = System.nanoTime() - start;
        System.err.printf("hash using java cost:  %d %d %n", cost, cost / ITERATIONS / 1000);
    }
}