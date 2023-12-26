package tech.bsdb.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public interface AsyncFileReader {
    int QD = 512;
    int DEFAULT_TIMEOUT = 1000; //1 us
    int MAX_TIMEOUT = 10 * 1000 * 1000;//1ms

    void start();

    /**
     * Caution: the buffer will be reused, so should not be accessed after handler.completed() returned
     *
     * @param fd
     * @param position
     * @param handler
     * @throws InterruptedException
     */
    void read(int fd, long position, CompletionHandler<ByteBuffer, Integer> handler) throws InterruptedException;

    void read(int fd, long position, int readLen, CompletionHandler<ByteBuffer, Integer> handler) throws InterruptedException;

    void close() throws IOException;

    class ReadOperation {
        int fd;
        long reqId;
        long readPosition;
        ByteBuffer readBuffer;
        int offset;
        int limit;
        CompletionHandler<ByteBuffer, Integer> handler;
        int readResponse;
        Object attach;
        int alignedReadSize;
    }
}
