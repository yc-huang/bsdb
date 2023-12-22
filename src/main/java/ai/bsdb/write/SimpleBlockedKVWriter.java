package ai.bsdb.write;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class SimpleBlockedKVWriter extends BlockedKVWriter {
    private final File[] dataFiles;
    private final SeekableByteChannel[] channels;
    private final ByteBuffer[] readBuffers;

    Logger logger = LoggerFactory.getLogger(SimpleBlockedKVWriter.class);

    public SimpleBlockedKVWriter(File dataFile) throws IOException {
        this(dataFile, DEFAULT_BLOCK_SIZE, DEFAULT_PARTITION_NUM);
    }

    public SimpleBlockedKVWriter(File dataFile, int blockSize, int partitionNum) throws IOException {
        super(blockSize, partitionNum);
        this.dataFiles = new File[partitionNum];
        this.channels = new SeekableByteChannel[partitionNum];
        this.readBuffers = new ByteBuffer[partitionNum];
        for (int partition = 0; partition < partitionNum; partition++) {
            this.dataFiles[partition] = getPartitionFile(dataFile, partition);
            this.channels[partition] = Files.newByteChannel(this.dataFiles[partition].toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            this.readBuffers[partition] = ByteBuffer.allocateDirect(blockSize);
        }
    }

    @Override
    void flushBlocks0(int partition, ByteBuffer buffer) throws IOException {
        this.channels[partition].write(buffer);
    }

    @Override
    void finishPartiton0(int partition) throws IOException {
        //this.channels[partition].close();
    }

    @Override
    ByteBuffer readBlockAt(int partition, long position) throws IOException {
        ByteBuffer buffer = this.readBuffers[partition];
        buffer.clear();
        this.channels[partition].position(position);
        int r = this.channels[partition].read(buffer);
        if (r > 0) {
            buffer.position(0);
            buffer.limit(r);
            return buffer;
        } else return null;
    }

}
