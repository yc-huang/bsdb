package ai.bsdb.write;

import ai.bsdb.io.NativeFileIO;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class BlockedKVWriter extends PartitionedKVWriter {
    protected final static int DEFAULT_BLOCK_SIZE = NativeFileIO.PAGE_SIZE;
    final int blockSize;
    final ByteBuffer[] writeBuffers;
    final ByteBuffer[] largeWriteBuffers;

    /**
     * group records in batch and write out as blocks. Records with size less than block size will be put in a block if there's enough free space,
     * or to the beginning of a new block. Records that larger than a block will be written to a large block with size (block size + min(n) * page size > record siz).
     * All writes to disk are at the size of a block or huge block, so that might leave some space in a block unused and wasted.
     *
     * @param blockSize    size in Bytes of a block, should be multiplied of 4096
     * @param partitionNum partitions
     */
    public BlockedKVWriter(int blockSize, int partitionNum) {
        super(partitionNum);
        if (blockSize % NativeFileIO.PAGE_SIZE != 0)
            throw new RuntimeException("bad block size, " + blockSize + " not divide by " + NativeFileIO.PAGE_SIZE);
        this.blockSize = blockSize;
        this.writeBuffers = new ByteBuffer[partitionNum];
        this.largeWriteBuffers = new ByteBuffer[partitionNum];
        for (int i = 0; i < partitionNum; i++) {
            this.writeBuffers[i] = ByteBuffer.allocateDirect(blockSize);
        }
    }

    @Override
    void putToPartition(int partition, byte[] key, byte[] value) throws IOException {
        writeRecord(partition, this.writeBuffers[partition], key, value);
    }

    @Override
    void flushPartition(int partition) throws IOException {
        flushBlocks(partition, this.writeBuffers[partition]);
        finishPartiton0(partition);
    }

    void writeRecord(int partition, ByteBuffer tempBuf, byte[] key, byte[] value) throws IOException {
        int recLen = getRecordLength(key, value);
        if (recLen > tempBuf.capacity()) {
            //large records
            int nextBlockSize = alignRecordSizeToPageSize(recLen);
            if (largeWriteBuffers[partition] == null || largeWriteBuffers[partition].capacity() < nextBlockSize) {

            }
            ByteBuffer largeBuf = ByteBuffer.allocateDirect(nextBlockSize);
            writeRecord2Buffer(key, value, largeBuf);
            flushBlocks(partition, largeBuf);
        } else {
            if (tempBuf.remaining() < recLen) {
                flushBlocks(partition, tempBuf);
                tempBuf.clear();
            }
            writeRecord2Buffer(key, value, tempBuf);
        }
    }

    void flushBlocks(int partition, ByteBuffer buffer) throws IOException {
        if (buffer.position() > 0) {
            if (buffer.remaining() > 0) {
                buffer.put((byte) 0); //mark data finished
            }
            buffer.position(0);
            buffer.limit(buffer.capacity());
            flushBlocks0(partition, buffer);
        }
    }

    protected int alignRecordSizeToPageSize(int recLen) {
        return NativeFileIO.alignToPageSize(recLen);
    }

    abstract void flushBlocks0(int partition, ByteBuffer buffer) throws IOException;

    abstract void finishPartiton0(int partition) throws IOException;

    long partitionForEach(int partition, ScanHandler handler) throws IOException {
        ByteBuffer buf;
        long position = 0;
        long recordCount = 0;
        int nextBlockDelta;
        int recordOffset = 0;
        while ((buf = readBlockAt(partition, position)) != null) {
            nextBlockDelta = blockSize;
            while (buf.remaining() > 0) {
                recordOffset = buf.position();
                int keyLen = readKeyLength(buf);
                if (keyLen > 0) {
                    recordCount++;
                    int valueLen = readValueLength(buf);
                    byte[] key = new byte[keyLen];
                    buf.get(key);

                    if (valueLen <= buf.remaining()) {
                        byte[] value = new byte[valueLen];
                        buf.get(value);
                        handler.handleRecord(getRecordAddress(partition, blockSize, position, recordOffset), key, value);
                    } else {
                        //large block
                        nextBlockDelta = alignRecordSizeToPageSize(getRecordLength(keyLen, valueLen));
                        handler.handleRecord(getRecordAddress(partition, nextBlockDelta, position, recordOffset), key, null);//TODO: read value
                        break;
                    }
                } else {
                    //no more records
                    break;
                }
            }

            position += nextBlockDelta;
        }

        return recordCount;
    }

    /**
     * generate block address to store in index.
     * the first byte store the partition number
     * the second byte stores the block size, in pages
     * the 3th-6th bytes stores the block's start position in file, in pages
     * the last two bytes stores the record offset in block, in bytes
     *
     * @param blockPosition
     * @param recordOffset
     * @return
     */
    protected long getRecordAddress(int partition, int blockSize, long blockPosition, int recordOffset) {
        return (long) partition << 56 | (long) (blockSize / NativeFileIO.PAGE_SIZE) << 48 | (blockPosition / NativeFileIO.PAGE_SIZE) << 16 | recordOffset;
    }

    abstract ByteBuffer readBlockAt(int partition, long position) throws IOException;
}
