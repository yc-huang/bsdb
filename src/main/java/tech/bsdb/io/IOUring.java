package tech.bsdb.io;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

/**
 * IoURing high level interface
 * <p>
 * capabilities:
 * <p>
 * 0) setup and customize IOURing instance
 * a) io-poll for (nvme + xfs)
 * b) sq-poll     (reduce syscall)
 * <p>
 * 1) submit read/write
 * a) against heap byte[]
 * b) against DirectByteBuffer
 * 2) wait request completion
 * 3) batch submit read/write requests
 * a) multi-buffer per file
 * b) multi-io
 * 4) wait batch completion
 * <p>
 * 5) register DirectByteBuffer to use Fixed read/write ( for performance )
 * 6) register Files to use Fixed file ( for performance )
 * <p>
 * 7) buffer conversion:
 * 1) read/write base on Heap bytes array
 * 2) read/write base on DirectByteBuffer
 *
 * <h2>Read/Write flow</h2>
 * 1) get Submission Queue entry will return the <b>request-Id</b>
 * internally, we associate the SQE with a request-Id (cookie)
 * 2) prepare read/write/readv/writev against request-Id
 * 3) goto 1) until all read/write prepared
 * 4) submit prepared ios in the Submission Queue
 * 5) query or wait completion of submitted requests in the Queue.
 */
public class IOUring {
    Native.Uring _native;
    int flags;

    public static class IOResult {
        public final long reqId;
        public final long res;

        public IOResult(long reqId, long res) {
            this.reqId = reqId;
            this.res = res;
        }
    }

    public IOUring(int queueDepth, int flags) {
        this._native = new Native.Uring(queueDepth, flags);
        this.flags = flags;
    }

    public void shutdown() {
        this._native.exitUring();
    }

    /**
     * high level wrappers
     * <p>
     * READ/WRITE
     * <p>
     * REQUEST/COMPLETION
     */

    public int availableSQ() {
        return _native.availableSQ();
    }

    public void prepareRead(long reqId, int fd, long offset, ByteBuffer dBuf, int len) {
        int ret = _native.prepareRead(reqId, 0, fd, ((DirectBuffer)dBuf).address(), len, offset);
        if (ret != 0) {
            throw new RuntimeException("IOUring.prepareRead failed with ret: " + ret);
        }
    }

    public void prepareReadFixed(long reqId, int fd, long offset, ByteBuffer dBuf, int len, int bufIndex){
        _native.prepareReadFixed(reqId, 0, fd,  ((DirectBuffer)dBuf).address(), len, offset, bufIndex);
    }

    public int prepareRead(long[] reqIds, int fd, long[] offsets, long[] dBufs, int[] lens, int from, int to) {
        return _native.prepareReadM(reqIds, 0, fd, dBufs, lens, offsets, from, to);
    }

    public int submit() {
        int ret = _native.submit();
        if (ret < 0) {
            throw new RuntimeException("IOUring.submit failed with ret: " + ret);
        }
        return ret;
    }

    public IOResult waitCQEntry() {
        return waitCQEntries(1);
    }

    public IOResult waitCQEntries(int nr) {
        long[] reqIds = new long[1], retCodes = new long[1];
        int ret = _native.waitCQEntries(reqIds, retCodes, nr);
        if (ret != 0) {
            throw new RuntimeException("IOUring.waitCQEntries failed with ret: " + ret);
        }
        return new IOResult(reqIds[0], retCodes[0]);
    }

    public void seenCQEntry(int n) {
        _native.advanceCQ(n);
    }

    public IOResult[] peekCQEntries(int nr) {
        long[] reqIds = new long[nr], retCodes = new long[nr];
        int cnt = _native.peekCQEntries(reqIds, retCodes, nr);
        IOResult[] result = new IOResult[cnt];
        for (int i = 0; i < cnt; i++) {
            result[i] = new IOResult(reqIds[i], retCodes[i]);
        }
        return result;
    }

    public long[][] peekCQEntriesNoop(int nr) {
        long[] reqIds = new long[nr], retCodes = new long[nr];
        int cnt = _native.peekCQEntries(reqIds, retCodes, nr);
        long[][] result = new long[2][cnt];
        System.arraycopy(reqIds, 0, result[0], 0, cnt);
        System.arraycopy(retCodes, 0, result[1], 0, cnt);
        return result;
    }

    public int peekCQEntriesNoop(long[][] ret) {
        long[] reqIds = ret[0], retCodes = ret[1];
        return _native.peekCQEntries(reqIds, retCodes, reqIds.length);
    }

    public IOResult waitCQEntryTimeout(long ms) {
        long[] reqIds = new long[1], retCodes = new long[1];
        int ret = _native.waitCQEntryTimeout(reqIds, retCodes, ms);
        if (ret != 0) {
            //throw new RuntimeException("IOUring.waitCQEntryTimeout failed with ret: " + ret);
            return null;
        }
        return new IOResult(reqIds[0], retCodes[0]);
    }

    public int registerFile(int[] fds){
        return _native.registerFiles(fds);
    }

    public int registerBuffer(ByteBuffer[] buffers){
        long[] addrs = new long[buffers.length];
        long[] lengths = new long[buffers.length];
        for(int i = 0; i < buffers.length; i++) {
            ByteBuffer buf = buffers[i];
            addrs[i] = ((DirectBuffer)buf).address();
            lengths[i] = buf.capacity();
        }
        return _native.registerBuffers(addrs, lengths);
    }
}

