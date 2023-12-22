package ai.bsdb.io;

import ai.bsdb.util.Common;
import scala.util.control.Exception;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NativeFileIO {
    public static final int PAGE_SIZE = 4096;
    public static int openForReadDirect(String file) throws IOException {
        return open(file, Native.O_DIRECT | Native.O_RDONLY | Native.O_NOATIME);
    }

    public static int open(String file, int flags) throws IOException {
        int fd = Native.open(file, flags);
        if (fd < 0) {
            throw new IOException("Error opening " + file);
        } else {
            return fd;
        }
    }

    public static void close(int fd) {
        Native.close(fd);
    }

    public static int getBufferSizeForUnalignedRead(int readLen){
        return alignToPageSize(readLen + PAGE_SIZE);
    }

    /**
     * align to OS page size
     *
     * @param size
     * @return aligned size
     */
    public static int alignToPageSize(int size) {
        int r = size / PAGE_SIZE * PAGE_SIZE;
        if (r < size) r += PAGE_SIZE;
        return r;
    }

    public static ByteBuffer allocateAlignedBuffer(int size){
        return allocateAlignedBuffer(size, Common.MEMORY_ALIGN);
        //return ByteBuffer.allocateDirect(size + Common.MEMORY_ALIGN*2).alignedSlice(Common.MEMORY_ALIGN);
    }

    protected static ByteBuffer allocateAlignedBuffer(int size, int alignTo) {
        long addr = Native.allocateAligned(size, alignTo);
        if (addr > 0) {
            return UnsafeUtil.newDirectByteBuffer(addr, size, addr);
        } else {
            throw new RuntimeException("alloc failed");
        }
    }

    public static void freeBuffer(ByteBuffer buffer) {
        //Native.free(((DirectBuffer) buffer).address());
    }

    public static long readAlignedTo512(int fd, long position, ByteBuffer dst, int len) throws IOException {
        return readAligned(fd, position, dst, len, -512L);
    }

    public static long readAlignedTo4096(int fd, long position, ByteBuffer dst, int len) throws IOException {
        return readAligned(fd, position, dst, len, -4096L);
    }

    private static long readAligned(int fd, long position, ByteBuffer dst, int len, long lenAlignMask) throws IOException {
        long alignedPos = position & -4096L; //position align to 4096
        int offset = (int) (position - alignedPos);
        int readLen = len + offset;
        int alignedLen = (int) (readLen & lenAlignMask); //read len align to
        if (alignedLen < readLen) alignedLen += (int) (-lenAlignMask);
        if (dst.remaining() < alignedLen)
            throw new IOException("no enough space in buffer to contain " + alignedLen + ", space remain " + dst.remaining() + " " + dst.position());

        long rLen = Native.pread(fd, alignedPos, ((DirectBuffer) dst).address(), alignedLen);
        if (rLen < 0) {
            throw new IOException("read return error:" + rLen);
        } else {
            dst.limit((int) rLen);
            dst.position(offset);
            return rLen;
        }
    }
}
