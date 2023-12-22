package ai.bsdb.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.LockSupport;

public class UringAsyncFileReader extends BaseAsyncFileReader {
    IOUring[] rings;
    long[][][] rss;
    Logger logger = LoggerFactory.getLogger(UringAsyncFileReader.class);

    public UringAsyncFileReader(int maxReadSize, int submitThreads, String tag) {
        super(maxReadSize, submitThreads, 0, tag);
        rings = new IOUring[submitThreads];
        rss = new long[submitThreads][][];
        for (int i = 0; i < submitThreads; i++) {
            rings[i] = new IOUring(QD, 0);
            rss[i] = new long[][]{new long[QD], new long[QD]};
            rings[i].registerBuffer(getPartitionPooledBuffer(i));
        }
    }


    public void registerFile(int[] fds) {
        for (IOUring ring : rings) ring.registerFile(fds);
    }

    @Override
    void submitRequest(int partition, ReadOperation[] reqs, int num) {
        for (int i = 0; i < num; i++) {
            ReadOperation op = reqs[i];
            op.readBuffer = getPartitionPooledBuffer(partition)[i];
            op.readBuffer.clear();
            op.reqId = i;
            //this.resultMaps[partition].put(op.reqId, op);
        }
        int cur = 0;
        while (num > cur) {
            int c = submit0(rings[partition], reqs, cur, num);
            cur += c;
            //if(c == 0) pollResponseAndCallback(partition, resultMap);//
        }

        long[][] rs = rss[partition];

        int resp = 0;
        while (resp < num) {
            //wait results for all submitted requests
            int c = rings[partition].peekCQEntriesNoop(rs);
            if (c > 0) {
                for (int i = 0; i < c; i++) {
                    long reqId = rs[0][i];
                    int bytesRead = (int) rs[1][i];
                    ReadOperation op = reqs[(int) reqId];
                    if (op != null) {
                        callback(op, bytesRead);
                    } else {
                        logger.error("op should not be null, reqid:{}", reqId);
                    }
                }
                rings[partition].seenCQEntry(c);
                resp += c;
            }
        }
    }

    @Override
    boolean pollResponseAndCallback(int partition) {
        /*
        boolean progress = false;
        long[][] rs = rss[partition];
        int c = rings[partition].peekCQEntriesNoop(rs);
        if (c > 0) {
            for (int i = 0; i < c; i++) {
                long reqId = rs[0][i];
                int bytesRead = (int) rs[1][i];
                ReadOperation op = this.resultMaps[partition].remove(reqId);
                if (op != null) {
                    callback(op, bytesRead);
                } else {
                    //logger.error("op should not be null, reqid:{}, resultMap size:{}", reqId, resultMaps[partition].mappingCount());
                }
            }
            rings[partition].seenCQEntry(c);
            progress = true;
        }

        return progress;
         */
        return false;
    }

    @Override
    void close0() {
        for (IOUring ring : rings) {
            ring.shutdown();
        }
    }

    private int submit0(IOUring ring, ReadOperation[] ops, int from, int to) {
        int freeSqs = ring.availableSQ();
        if (freeSqs <= 0) {
            LockSupport.parkNanos(100);
            return 0;
        } else {
            int available = Math.min(from + freeSqs, to);

            for (int i = from; i < available; i++) {
                ReadOperation op = ops[i];
                //ring.prepareRead(op.reqId, op.fd, op.readPosition, op.readBuffer, op.alignedReadSize);
                ring.prepareReadFixed(op.reqId, op.fd, op.readPosition, op.readBuffer, op.alignedReadSize, (int) op.reqId);//reqId is the same as buffer index
            }
            int s = ring.submit();
            return Math.max(s, 0);
        }
    }
}
