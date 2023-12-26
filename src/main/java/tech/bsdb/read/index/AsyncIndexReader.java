package tech.bsdb.read.index;

import tech.bsdb.read.SyncReader;
import tech.bsdb.read.kv.KVReader;

import java.nio.channels.CompletionHandler;

public interface AsyncIndexReader {
    boolean asyncGetAddrAt(long index, KVReader kvReader, byte[] key, CompletionHandler<byte[], Object> appHandler, Object appAttach, SyncReader.IndexCompletionHandler handler) throws InterruptedException;
    long size();
}
