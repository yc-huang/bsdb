package ai.bsdb.write;

import ai.bsdb.io.NativeFileIO;
import ai.bsdb.util.Common;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class KVWriterCompressed extends PartitionedKVWriter {
    private final BlockedCompression[] partitions;

    private byte[] dict;
    private final int dictSize;
    private final File basePath;
    private final static int MIN_SHARED_DICT_SIZE = 64 * 1024;

    Logger logger = LoggerFactory.getLogger(KVWriterCompressed.class);

    public KVWriterCompressed(File dataFile, int compressBlockSize, int sharedDictSize, boolean truncate) throws IOException {
        this(dataFile, compressBlockSize, sharedDictSize, DEFAULT_PARTITION_NUM, truncate);
    }

    public KVWriterCompressed(File dataFile, int compressBlockSize, int sharedDictSize, int partitions, boolean truncate) throws IOException {
        super(partitions);
        this.partitions = new BlockedCompression[partitions];
        for (int partition = 0; partition < partitions; partition++) {
            this.partitions[partition] = new BlockedCompression(partition, getPartitionFile(dataFile, partition), compressBlockSize, 6, truncate);
        }
        dictSize = Math.max(MIN_SHARED_DICT_SIZE, sharedDictSize);
        basePath = dataFile.getParentFile();
    }

    class BlockedCompression {
        File file;
        ByteChannel channel;
        //int blockNo = 0;
        //long currentBlockAddr = 0;
        int blockOffset = 0;
        int blockSize; //max 64K
        final int BLOCK_HEADER_LEN = 8;
        int BLOCK_CONTENT_LIMIT;

        ByteBuffer compressedBlock;
        ByteBuffer decompressedBlock;

        ByteBuffer tempBuf;
        ByteBuffer largeTempBuf;

        ZstdCompressCtx compressZstd;
        ZstdDecompressCtx decompressZstd;
        final int partitionNum;

        BlockedCompression(int bucketNum, File out, int compressBlockSize, int level, boolean truncate) throws IOException {
            this.partitionNum = bucketNum;
            this.file = out;
            this.channel = Files.newByteChannel(out.toPath(), StandardOpenOption.CREATE, truncate ? StandardOpenOption.TRUNCATE_EXISTING : StandardOpenOption.READ, StandardOpenOption.WRITE);

            blockSize = compressBlockSize;
            BLOCK_CONTENT_LIMIT = findUncompressedBoundFor(blockSize - BLOCK_HEADER_LEN);
            //logger.info("BLOCK LIMIT:{}", BLOCK_CONTENT_LIMIT);
            compressedBlock = ByteBuffer.allocateDirect(blockSize);
            decompressedBlock = ByteBuffer.allocateDirect(blockSize);
            tempBuf = ByteBuffer.allocateDirect(blockSize);
            tempBuf.limit(BLOCK_CONTENT_LIMIT);
            //largeTempBuf = ByteBuffer.allocateDirect(blockSize);

            this.compressZstd = new ZstdCompressCtx();
            this.compressZstd.setLevel(level);
            this.decompressZstd = new ZstdDecompressCtx();

        }

        void setDict(byte[] dict) {
            this.compressZstd.loadDict(dict);
            this.decompressZstd.loadDict(dict);
        }

        void writeRecord(byte[] key, byte[] value) throws IOException {
            int recLen = key.length + value.length + Common.RECORD_HEADER_SIZE;

            if (recLen > this.tempBuf.limit()) {
                //large record
                int nextBlockSize = NativeFileIO.alignToPageSize(recLen);
                if (this.largeTempBuf == null || nextBlockSize > this.largeTempBuf.capacity()) {
                    this.largeTempBuf = ByteBuffer.allocateDirect(nextBlockSize);
                    this.decompressedBlock = ByteBuffer.allocateDirect(nextBlockSize);
                    this.compressedBlock = ByteBuffer.allocateDirect(findCompressedBoundFor(nextBlockSize) + BLOCK_HEADER_LEN);//unaligned
                    this.blockSize = this.compressedBlock.capacity();
                }
                writeRecord2Buffer(key, value, this.largeTempBuf);
                flush(this.largeTempBuf);
            } else {
                if (this.tempBuf.remaining() < recLen) {
                    flush(this.tempBuf);
                }
                writeRecord2Buffer(key, value, this.tempBuf);
            }
        }

        protected void writeRecord2Buffer(byte[] key, byte[] value, ByteBuffer buf) {
            try{buf.put((byte) key.length);
            buf.putShort((short) value.length);
            buf.put(key);
            buf.put(value);}catch (BufferOverflowException boe){
                logger.error("write {}:{} to {} failed",key.length, value.length, buf, boe);
                throw boe;
            }
        }

        //find the max uncompressed size that 100% could be compressed to a buffer of targetLen size
        private int findUncompressedBoundFor(int targetLen) {
            int guess = targetLen;
            long bound = Zstd.compressBound(guess);
            while (bound > targetLen) {
                guess -= 1;
                bound = Zstd.compressBound(guess);
            }
            //logger.debug("zstd compress {} -> {}", guess, targetLen);
            return guess;
        }

        //find the max compressed size of input with targetLen size
        private int findCompressedBoundFor(int targetLen) {
            return (int) Zstd.compressBound(targetLen);
        }

        protected long getRecordAddress(long blockAddress, int blockOffset) {
            return (long) partitionNum << 56 | blockAddress << 16 | blockOffset;
        }

        void flush(ByteBuffer buf) throws IOException {
            try {
                int origLen = buf.position();
                int len = this.compressZstd.compressDirectByteBuffer(this.compressedBlock, BLOCK_HEADER_LEN, this.compressedBlock.capacity()-BLOCK_HEADER_LEN, buf, 0, buf.position());
                //System.err.println("compressed len:" + len);
                //assert len > 0 && len <= this.tempBuf.position();
                this.compressedBlock.putShort(0, (short) len);
                this.compressedBlock.putShort(2, (short) origLen);

                int blockLen = len + BLOCK_HEADER_LEN;
                this.compressedBlock.position(0);
                this.compressedBlock.limit(blockLen);
                this.channel.write(this.compressedBlock);

                KVWriterCompressed.this.statistics.onBlock(origLen, blockLen);
            }catch (ZstdException e){
                logger.error("compress {} -> {} error, some records will be lost!!!", buf,compressedBlock, e);
                throw  e;
            }finally {
                buf.position(0);
            }
        }

        void finish() throws IOException {
            flush(this.tempBuf);
        }

        long forEach(ScanHandler handler) throws IOException {
            MMapBuffer mmap = new MMapBuffer(this.file, MMapMode.READ_ONLY);
            long size = mmap.size();
            logger.info("scan data file {} with length {}", file, size);

            long addr = 0;
            int blockCount = 0;
            long totalCost = 0;
            long blockBytes = 0;
            long compressedBlockBytes = 0;
            long records = 0;

            while (addr < size) {
                long start = System.nanoTime();
                ByteBuffer bb = mmap.toDirectByteBuffer(addr, blockSize);
                int blockLen = bb.getShort(0) & 0xFFFF;
                int decompressedLen = bb.getShort(2) & 0XFFFF;

                assert blockLen > 0 && blockLen <= decompressedBlock.capacity();
                //bb.limit(blockLen + BLOCK_HEADER_LEN);
                try {
                    //lz4 decompress need to specify the original decompressed length
                    int len = decompressZstd.decompressDirectByteBuffer(this.decompressedBlock, 0, decompressedBlock.capacity(), bb, BLOCK_HEADER_LEN, blockLen);
                    assert len == decompressedLen;
                    this.decompressedBlock.rewind();
                    int pos = 0;
                    while (pos < len) {
                        int keyLen = this.decompressedBlock.get() & 0xFF;
                        int valueLen = this.decompressedBlock.getShort() & 0xFFFF;
                        byte[] key = new byte[keyLen];
                        byte[] value = new byte[valueLen];
                        this.decompressedBlock.get(key);
                        this.decompressedBlock.get(value);
                        handler.handleRecord(getRecordAddress(addr, pos), key, value);
                        pos += keyLen + valueLen + Common.RECORD_HEADER_SIZE;
                        records++;
                    }

                    assert len > 0 && len <= BLOCK_CONTENT_LIMIT;
                    totalCost += System.nanoTime() - start;
                    blockCount++;
                    blockBytes += len;
                    compressedBlockBytes += blockLen;
                    addr += blockLen + BLOCK_HEADER_LEN;
                }catch (ZstdException zstde){
                    logger.error("{} - {}, buf:{}->{}",blockLen, decompressedLen, bb,decompressedBlock,zstde);
                    throw zstde;
                }
                catch (Exception e) {
                    logger.error("failed to scan compressed kv file", e);
                    throw  e;
                }
            }
            logger.info("scan found {} records. Avg block decompressed cost {} us, avg block size:{}, compress ratio:{}", records, (totalCost) / blockCount / 1000.0, blockBytes / blockCount, compressedBlockBytes * 1.0 / blockBytes);
            return records;
        }
    }


    @Override
    protected void sample0(List<byte[]> keys, List<byte[]> values) {
        ByteBuffer buf = ByteBuffer.allocateDirect(16 * 1024 * 1024);
        int[] inputSizes = new int[keys.size()];
        int sampleCount = 0;
        for (int i = 0; i < keys.size(); i++) {
            byte[] key = keys.get(i);
            byte[] value = values.get(i);
            int recSize = getRecordLength(key, value);
            if (buf.remaining() >= recSize) {
                writeRecord2Buffer(key, value, buf);
                inputSizes[i] = recSize;
                sampleCount++;
            } else {
                break;
            }
        }
        int[] sizes = new int[sampleCount];
        System.arraycopy(inputSizes, 0, sizes, 0, sampleCount);

        dict = new byte[dictSize];
        ByteBuffer dictBuf = ByteBuffer.allocateDirect(dictSize);
        long s = Zstd.trainFromBufferDirect(buf, sizes, dictBuf);
        dict = new byte[(int) s];
        dictBuf.get(dict);
        //only use values for dict tranin. TODO: in some case keys might also be considered to train dict
        //Zstd.trainFromBuffer(values.stream().filter(Objects::nonNull).toArray(byte[][]::new), dict); //TODO: might need to trim dict according to returned size
        for (BlockedCompression partition : partitions) partition.setDict(dict);
        try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(new File(basePath, Common.FILE_NAME_SHARED_DICT).toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))) {
            oos.writeObject(dict);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        ByteBuffer dst = ByteBuffer.allocateDirect(buf.capacity() + 64);
        long compressedSize = Zstd.compressDirectByteBufferUsingDict(dst, 0, dst.capacity(), buf, 0, buf.position(), dict, 6);
        double ratio = buf.position() * 1.0 / compressedSize;

        logger.info("compress ratio of samples:{}", ratio);
    }

    @Override
    void putToPartition(int partition, byte[] key, byte[] value) throws IOException {
        //logger.info("put {}->{}", key, value);
        partitions[partition].writeRecord(key, value);
    }

    @Override
    void flushPartition(int partition) throws IOException {
        partitions[partition].finish();
    }

    @Override
    long partitionForEach(int partition, ScanHandler handler) throws IOException {
        return partitions[partition].forEach(handler);
    }
}
