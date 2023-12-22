package ai.bsdb.read.index;

import ai.bsdb.read.SyncReader;
import ai.bsdb.read.kv.KVReader;

import java.nio.channels.CompletionHandler;

public interface AsyncIndexReader {
    boolean asyncGetAddrAt(long index, KVReader kvReader, byte[] key, CompletionHandler<byte[], Object> appHandler, Object appAttach, SyncReader.IndexCompletionHandler handler) throws InterruptedException;
    long size();
}
