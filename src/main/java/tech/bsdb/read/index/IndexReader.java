package tech.bsdb.read.index;

public interface IndexReader {
    long getAddrAt(long index) throws Exception;
    byte[] getRawBytesAt(long index)throws Exception;
    long size();
}
