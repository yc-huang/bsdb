package tech.bsdb.util;

import tech.bsdb.io.NativeFileIO;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueueModified;
import com.conversantmedia.util.concurrent.MPMCBlockingQueue;
import com.github.luben.zstd.ZstdInputStream;
import sun.misc.Unsafe;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.buffer.UnsafeUtil;
import xerial.larray.mmap.MMapBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;

public class Common {
    public static final String FILE_NAME_KV_DATA = "kv.db";
    public static final String FILE_NAME_KEY_HASH = "hash.db";
    public static final String FILE_NAME_KV_INDEX = "index.db";
    public static final String FILE_NAME_KV_APPROXIMATE_INDEX = "index_a.db";
    public static final String FILE_NAME_CONFIG = "config.properties";
    public static final String FILE_NAME_SHARED_DICT = "shared_dict";
    public static final String FILE_NAME_VALUE_SCHEMA = "value.schema";

    public static final String CONFIG_KEY_KV_COMPRESS = "kv.compressed";
    public static final String CONFIG_KEY_KV_COMPACT = "kv.compact";
    public static final String CONFIG_KEY_KV_COMPRESS_BLOCK_SIZE = "kv.compress.block.size";
    public static final String CONFIG_KEY_APPROXIMATE_MODE = "index.approximate";
    public static final String CONFIG_KEY_CHECKSUM_BITS = "hash.checksum.bits";
    public static final String CONFIG_KEY_KV_RECORD_LEN_MAX = "kv.key.len.max";
    public static final String CONFIG_KEY_KV_RECORD_LEN_AVG = "kv.key.len.avg";
    public static final String CONFIG_KEY_KV_KEY_LEN_MAX = "kv.key.len.max";
    public static final String CONFIG_KEY_KV_KEY_LEN_AVG = "kv.key.len.avg";
    public static final String CONFIG_KEY_KV_COUNT = "kv.count";
    public static final String CONFIG_KEY_KV_VALUE_LEN_MAX = "kv.value.len.max";
    public static final String CONFIG_KEY_KV_VALUE_LEN_AVG = "kv.value.len.avg";
    public static final String CONFIG_KEY_KV_BLOCK_COMPRESS_LEN_MAX = "kv.block.compress.max";
    public static final String CONFIG_KEY_KV_BLOCK_COMPRESS_LEN_AVG = "kv.block.compress.avg";
    public static final String CONFIG_KEY_KV_BLOCK_LEN_MAX = "kv.block.max";
    public static final String CONFIG_KEY_KV_BLOCK_LEN_AVG = "kv.block.avg";

    public static final int SLOT_SIZE = 8;
    public static final int MAX_RECORD_SIZE = 32768;//MAX_KEY_SIZE + MAX_VALUE_SIZE + RECORD_HEADER_SIZE; //should be multiply of 512
    public static final int RECORD_HEADER_SIZE = 3;
    public static final int MAX_KEY_SIZE = 255;
    public static final int MAX_VALUE_SIZE = MAX_RECORD_SIZE - MAX_KEY_SIZE - RECORD_HEADER_SIZE;

    public static final int DEFAULT_BLOCK_SIZE = 4096; //should be multiply of 4096

    public static final int MEMORY_ALIGN = 4096;

    public final static boolean REVERSE_ORDER = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    public final static int CPUS = Runtime.getRuntime().availableProcessors();
    public static final int DEFAULT_COMPRESS_BLOCK_SIZE = 8192;

    private static final byte[] DIGITS = {
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
            -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    };
    public static final int DEFAULT_THREAD_POOL_QUEUE_SIZE = Integer.parseInt(System.getProperty("bsdb.queue.base.size", "1024"));

    public final static Unsafe unsafe = UnsafeUtil.getUnsafe();

    /**
     * check if two byte[] have the same content
     *
     * @param a
     * @param b
     * @return
     */
    public static boolean bytesEquals(byte[] a, byte[] b) {
        if (a != null && b != null && a.length == b.length) {
            for (int i = 0; i < a.length; i++) {
                if (a[i] != b[i]) return false;
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * check if a byte[] and a ByteBuffer have the same content
     *
     * @param a
     * @param b
     * @return
     */
    public static boolean bytesEquals(byte[] a, ByteBuffer b) {
        if (a != null && b != null && a.length == b.limit()) {
            for (int i = 0; i < a.length; i++) {
                if (a[i] != b.get(i)) return false;
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * @param buf
     * @param startPos
     * @param endPos
     * @param b
     * @return
     */
    public static long indexOf(MMapBuffer buf, long startPos, long endPos, byte b) {
        long pos = startPos;
        while (pos < endPos) {
            byte r = buf.getByte(pos);
            if (r == b) {
                return pos;
            } else {
                pos++;
            }
        }
        return -1;
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    /**
     * convert bytes to it's HEX format, each byte will be converted to 2 chars
     *
     * @param bytes
     * @return hex format of input bytes
     */
    public static char[] bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return hexChars;
    }

    /**
     * convert HEX format back to bytes, 0 might be added to the first byte if input length is not even
     *
     * @param hex
     * @return bytes of input hex string
     */
    public static byte[] hexToBytes(String hex) {
        return hexToBytes(hex.toCharArray());
    }

    public static byte[] hexToBytes(char[] hex) {
        int len = hex.length;
        boolean prefix = false;
        if ((len & 0x2) != 0) {
            len++;
            prefix = true;
        }
        len = len >>> 1;
        byte[] ret = new byte[len];

        int startIndex = 0;
        if (prefix) {
            ret[0] = fromHexByte((char) 0, hex[startIndex]);
        } else {
            ret[0] = fromHexByte(hex[startIndex++], hex[startIndex++]);
        }

        for (int j = 1; j < len; j++) {
            ret[j] = fromHexByte(hex[startIndex++], hex[startIndex++]);
        }
        return ret;
    }

    private static byte fromHexByte(char h, char l) {
        int high = fromHexDigit(h);
        int low = fromHexDigit(l);
        return (byte) ((high << 4) | low);
    }

    /**
     * check if a input byte is a valid part of a HEX formatted (0-9,a-f,A-F)
     *
     * @param ch
     * @return
     */
    public static boolean isHexDigit(int ch) {
        return ((ch >>> 8) == 0 && DIGITS[ch] >= 0);
    }

    private static int fromHexDigit(int ch) {
        int value;
        if ((ch >>> 8) == 0 && (value = DIGITS[ch]) >= 0) {
            return value;
        }
        throw new NumberFormatException("not a hexadecimal digit: \"" + (char) ch + "\" = " + ch);
    }


    public static final int MAX_POWER2 = (1 << 30);

    /**
     * return the next power of two after @param capacity or
     * capacity if it is already
     */
    public static int toPowerOf2(int capacity) {
        int c = 1;
        if (capacity >= MAX_POWER2) {
            c = MAX_POWER2;
        } else {
            while (c < capacity) c <<= 1;
        }

        if (isPowerOf2(c)) {
            return c;
        } else {
            throw new RuntimeException("Capacity is not a power of 2.");
        }
    }

    /*
     * define power of 2 slightly strangely to include 1,
     *  i.e. capacity 1 is allowed
     */
    private static boolean isPowerOf2(final int p) {
        return (p & (p - 1)) == 0;
    }


    /**
     * util method for getting cached direct bytebuffer from thread local for reuse
     *
     * @param threadLocal ThreadLocal to contain the cached ByteBuffer
     * @param size        the capacity of the ByteBuffer to allocate if not cached, should be consistent
     * @return a cached ByteBuffer
     */
    public static ByteBuffer getBufferFromThreadLocal(ThreadLocal<ByteBuffer> threadLocal, int size, boolean aligned) {
        ByteBuffer buf = threadLocal.get();
        if (buf == null) {
            buf = aligned ? NativeFileIO.allocateAlignedBuffer(size) : ByteBuffer.allocateDirect(size);//ByteBuffer.allocateDirect(size);
            threadLocal.set(buf);
        }
        buf.clear();
        return buf;
    }

    public static void copyTo(LBufferAPI mmap, long srcOffset, byte[] destArray, int destOffset, int size) {
        int cursor = destOffset;
        for (ByteBuffer bb : mmap.toDirectByteBuffers(srcOffset, size)) {
            int bbSize = bb.remaining();
            if ((cursor + bbSize) > destArray.length)
                throw new ArrayIndexOutOfBoundsException(String.format("cursor + bbSize = %,d", cursor + bbSize));
            bb.get(destArray, cursor, bbSize);
            cursor += bbSize;
        }
    }


    /**
     * util method to run tasks parallel and then wait for tasks finished.
     *
     * @param threads thread pool size to run the tasks
     * @param task    utility class to submit tasks and return Futures, e.g. (pool, futures) -> {futures.add(pool.submit(() -> {}))}
     */

    public static void runParallel(int threads, ParallelTask task, boolean waitForDone) {
        ExecutorService executor = new ThreadPoolExecutor(threads, threads, 1, TimeUnit.SECONDS, new DisruptorBlockingQueueModified<>(1024));
        List<Future> futures = new ArrayList<>();
        task.addTasks(executor, futures);
        if (waitForDone) {
            for (Future future : futures)
                try {
                    future.get();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
        }
        executor.shutdown();
    }

    public interface ParallelTask {
        void addTasks(ExecutorService pool, List<Future> futures);
    }

    /**
     * util method to shutdown a ForkJoinPool and blocking until all submitted tasks finished.
     *
     * @param pool
     */
    public static void waitForShutdown(ForkJoinPool pool) {
        pool.shutdown();
        while (!pool.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }

    public static int getPropertyAsInt(String propKey, int defaultValue) {
        return Integer.parseInt(System.getProperty(propKey, "" + defaultValue));
    }

    /**
     * return a thread pool with specified threads and default queue size
     *
     * @param num threads in pool
     * @return a new thread pool
     */
    public static ExecutorService fixedThreadPool(int num) {
        return fixedThreadPool(num, DEFAULT_THREAD_POOL_QUEUE_SIZE);
    }

    /**
     * return a thread pool with specified threads and queue size
     *
     * @param num      threads in pool
     * @param queueLen queue length for buffered tasks
     * @return a new thread pool
     */
    public static ExecutorService fixedThreadPool(int num, int queueLen) {
        return new ThreadPoolExecutor(num, num, 1, TimeUnit.SECONDS, new MPMCBlockingQueue<>(queueLen));
    }

    /**
     * get system free memory size
     *
     * @return available memory size in MB
     * @throws IOException
     */
    public static int freeMemory() throws IOException {
        Process p = new ProcessBuilder().command("free", "-m").start();
        InputStream out = p.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(out));
        reader.readLine();//header line
        String memLine = reader.readLine();
        String[] splits = memLine.split(" ");
        return Integer.parseInt(splits[splits.length - 1]);
    }

    /**
     * calc least-common-multiplier of two inputs
     *
     * @param x
     * @param y
     * @return least-common-multiplier of x,y
     */
    public static int lcm(long x, long y) {
        // will hold gcd
        long g = x;
        long yc = y;

        // get the gcd first
        while (yc != 0) {
            long t = g;
            g = yc;
            yc = t % yc;
        }

        return (int) (x * y / g);
    }

    public static BufferedReader getReader(File input, Charset charset) throws IOException {
        Path inPath = input.toPath();
        InputStream is = Files.newInputStream(inPath, StandardOpenOption.READ);
        String extension = com.google.common.io.Files.getFileExtension(input.getName());
        if ("gz".equals(extension)) {
            is = new GZIPInputStream(is);
        } else if ("zstd".equals(extension)) {
            is = new ZstdInputStream(is);
        }
        return new BufferedReader(new InputStreamReader(is, charset));
    }
}
