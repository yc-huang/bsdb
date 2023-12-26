package tech.bsdb.read.kv;

import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public interface KVReader {
    byte[] getValueAsBytes(long addr, byte[] key) throws Exception;
    boolean asyncGetValueAsBytes(long addr, byte[] key, CompletionHandler<byte[], byte[]> handler) throws InterruptedException;
}
