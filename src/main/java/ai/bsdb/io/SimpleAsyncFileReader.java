package ai.bsdb.io;

import java.io.IOException;

public class SimpleAsyncFileReader extends BaseAsyncFileReader {

    public SimpleAsyncFileReader(int readLen, int submitThreads, String tag) {
        super(readLen, submitThreads, 0, tag);
    }

    @Override
    void submitRequest(int partition, ReadOperation[] reqs, int num) throws IOException {
        for (int i = 0; i < num; i++) {
            ReadOperation op = reqs[i];
            op.readBuffer = getPartitionPooledBuffer(partition)[i];
            op.readBuffer.clear();
            int bytesRead = (int) NativeFileIO.readAlignedTo512(op.fd, op.readPosition, op.readBuffer, op.alignedReadSize);
            callback(op, bytesRead);
        }
    }

    @Override
    boolean pollResponseAndCallback(int bucket) {
        return false;
    }

    @Override
    void close0() {

    }


}
