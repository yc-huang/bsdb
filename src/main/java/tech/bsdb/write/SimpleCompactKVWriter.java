package tech.bsdb.write;

import tech.bsdb.util.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class SimpleCompactKVWriter extends PartitionedKVWriter {
    private final File[] dataFiles;
    private final BufferedOutputStream[] foss;

    Logger logger = LoggerFactory.getLogger(SimpleCompactKVWriter.class);

    public SimpleCompactKVWriter(File dataFile) throws IOException {
        this(dataFile, DEFAULT_PARTITION_NUM);
    }

    public SimpleCompactKVWriter(File dataFile, int bucketNum) throws IOException {
        super(bucketNum);
        this.dataFiles = new File[bucketNum];
        this.foss = new BufferedOutputStream[bucketNum];
        for(int bucket = 0; bucket < bucketNum; bucket++){
            this.dataFiles[bucket] = getPartitionFile(dataFile, bucket);
            this.foss[bucket] = new BufferedOutputStream(Files.newOutputStream(this.dataFiles[bucket].toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
        }
    }

    @Override
    void putToPartition(int partition, byte[] key, byte[] value) throws IOException {
        this.foss[partition].write(key.length);
        this.foss[partition].write(value.length >> 8 & 0xFF);
        this.foss[partition].write(value.length & 0xFF);
        this.foss[partition].write(key);
        this.foss[partition].write(value);
    }

    @Override
    void flushPartition(int partition) throws IOException {
        this.foss[partition].close();
    }

    @Override
    long partitionForEach(int partition, ScanHandler handler) throws IOException {
        long records = 0;
        try(BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(this.dataFiles[partition].toPath()))) {
            long addr = 0;
            long recCount = 0;
            byte[] buf = new byte[Common.MAX_RECORD_SIZE];

            int kLen;
            while ((kLen = bis.read()) != -1) {
                if (kLen == 0) break;
                recCount++;
                int vLen = bis.read() << 8 | bis.read();
                assert vLen > 0;
                int rLen = bis.read(buf, 0, kLen);
                byte[] key = Arrays.copyOf(buf, rLen);

                rLen = bis.read(buf, 0, vLen);
                byte[] value = Arrays.copyOf(buf, rLen);
                final long recAddr = addr;
                handler.handleRecord((long) partition << 56 | recAddr, key, value);
                addr += (kLen + vLen + Common.RECORD_HEADER_SIZE);
                records ++;
            }
            logger.trace("scan bucket {} found records {}", partition, recCount);

            return records;
        }

    }
}
