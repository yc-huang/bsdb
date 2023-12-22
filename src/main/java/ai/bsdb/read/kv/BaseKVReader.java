package ai.bsdb.read.kv;

import ai.bsdb.io.AsyncFileReader;
import ai.bsdb.io.NativeFileIO;
import ai.bsdb.io.SimpleAsyncFileReader;
import ai.bsdb.io.UringAsyncFileReader;
import ai.bsdb.util.Common;
import ai.bsdb.util.RecyclingBufferPool;
import org.apache.commons.configuration2.Configuration;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public abstract class BaseKVReader implements KVReader {
    protected Configuration config;
    public BaseKVReader(Configuration config) {
        this.config = config;
    }

    public byte[] getValueAsBytes(long addr, byte[] key) throws IOException {
        ByteBuffer record = readRecord(addr);
        try {
            int keyLen = record.get() & 0xFF;
            int vLen = record.getShort() & 0xFFFF;
            if (checkKey(record, keyLen, key)) {
                byte[] ret = new byte[vLen];
                record.get(ret);
                return ret;
            } else {
                return null;
            }
        } finally {
            returnBuffer(addr, record);
        }
    }

    @Override
    public boolean asyncGetValueAsBytes(long addr, final byte[] key, CompletionHandler<byte[], byte[]> handler) throws InterruptedException {
        return asyncReadRecord(addr, new CompletionHandler<ByteBuffer, Integer>() {
            @Override
            public void completed(ByteBuffer record, Integer readCount) {
                int keyLen = record.get() & 0XFF;
                int vLen = record.getShort() & 0xFFFF;
                byte[] ret = null;
                if (checkKey(record, keyLen, key)) {
                    ret = new byte[vLen];
                    record.get(ret);
                }
                //if(pool != null) pool.release(record);
                handler.completed(ret, key);
            }

            @Override
            public void failed(Throwable throwable, Integer a) {
                handler.failed(throwable, key);
            }
        });
    }

    protected abstract boolean asyncReadRecord(long addr, CompletionHandler<ByteBuffer, Integer> handler) throws InterruptedException;

    protected abstract ByteBuffer readRecord(long addr) throws IOException;

    protected abstract void returnBuffer(long addr, ByteBuffer buf);


    private final static int BYTE_ARRAY_BASE_OFFSET = Common.unsafe.arrayBaseOffset(byte[].class);

    private boolean checkKey(ByteBuffer record, int rLen, byte[] key) {
        if (rLen != key.length) {
            //not match
            return false;
        }

        int words = rLen >>> 3; //assume Long.BYTES = 8

        for (int i = 0; i < words << 3; i += 8) {
            long l = Common.unsafe.getLong(key, BYTE_ARRAY_BASE_OFFSET + i);
            if (Common.REVERSE_ORDER) l = Long.reverseBytes(l);
            if (l != record.getLong()) return false; //2 bytes for key/value length
        }
        for (int i = words << 3; i < rLen; i++) {
            if (key[i] != record.get()) return false;
        }

        return true;
    }

    private boolean checkKey(ByteBuffer record, int keyLen, ByteBuffer key) {
        //byte[] sKey = new byte[keyLen];
        if (keyLen != key.remaining()) {
            //not match
            //System.err.println("length not match:" + keyLen);
            return false;
        }

        int words = keyLen >>> 3;
        for (int i = 0; i < words; i++) {
            if (key.getLong() != record.getLong()) return false;
        }

        for (int i = key.position(); i < key.limit(); i++) {
            if (key.get() != record.get()) {
                //not match
                return false;
            }
        }
        return true;
    }
}
