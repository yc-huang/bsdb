package ai.bsdb.read.index;

import ai.bsdb.read.SyncReader;
import ai.bsdb.read.kv.KVReader;

import java.nio.channels.CompletionHandler;

public interface IndexReader {
    long getAddrAt(long index) throws Exception;
    byte[] getRawBytesAt(long index)throws Exception;
    long size();
}
