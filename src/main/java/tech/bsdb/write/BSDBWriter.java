package tech.bsdb.write;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.sux4j.io.ConcurrentBucketedHashStore;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunctionModified;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static tech.bsdb.util.Common.*;

public class BSDBWriter {
    private final File basePath;
    private final KVWriter kvWriter;
    private final int checksumBits;
    private final long passCacheSize;
    private final ConcurrentBucketedHashStore<byte[]> keys;
    private final AtomicLong recordCount = new AtomicLong(0);
    private final boolean approximateMode;
    Configuration config;
    FileBasedConfigurationBuilder<PropertiesConfiguration> configBuilder;
    final static Logger logger = LoggerFactory.getLogger(BSDBWriter.class);

    public BSDBWriter(File basePath, File tmpDir, int checksumBits, long passCacheSize, boolean compact, boolean compress, int compressBLockSize, int sharedDictSize, boolean approximateMode) throws Exception {
        this.basePath = basePath;
        File kvFile = new File(basePath, FILE_NAME_KV_DATA);
        this.kvWriter = compress ? new KVWriterCompressed(kvFile, compressBLockSize,sharedDictSize, false) : compact ? new SimpleCompactKVWriter(kvFile) : new SimpleBlockedKVWriter(kvFile);
        this.keys = new ConcurrentBucketedHashStore<>(TransformationStrategies.byteArray(), tmpDir);
        this.checksumBits = checksumBits;
        this.passCacheSize = passCacheSize;
        this.approximateMode = approximateMode;

        File configFile = new File(basePath, FILE_NAME_CONFIG);
        if (!configFile.exists()) configFile.createNewFile();
        configBuilder = new Configurations().propertiesBuilder(configFile);
        try {
            config = configBuilder.getConfiguration();

            config.setProperty(CONFIG_KEY_KV_COMPRESS, Boolean.toString(compress));
            config.setProperty(CONFIG_KEY_KV_COMPACT, Boolean.toString(compact));
            config.setProperty(CONFIG_KEY_KV_COMPRESS_BLOCK_SIZE, compressBLockSize);
            config.setProperty(CONFIG_KEY_APPROXIMATE_MODE, Boolean.toString(approximateMode));
            config.setProperty(CONFIG_KEY_CHECKSUM_BITS, checksumBits);
        } catch (Exception e) {
            logger.error("RDBWriter init failed.", e);
            throw e;
        }

        //hashWriterPool = new ForkJoinPool();//maybe should set parallelism equal to segment size of BucketedHashStore, that's 256
    }

    public void sample(byte[] key, byte[] value) {
        kvWriter.sample(key, value);
    }

    public void onSampleFinished() {
        kvWriter.onSampleFinished();
    }

    public void put(byte[] key, byte[] value) throws IOException, InterruptedException {
        if (Objects.isNull(key) || Objects.isNull(value)) {
            logger.debug("Null key or value specified, key:{}, value:{}", key, value);
            throw new RuntimeException("currently null key/value is not support.");
        }
        this.kvWriter.put(key, value);
        //hashWriterPool.submit(() -> {
        //    try {
        this.keys.add(key);
        //    } catch (IOException e) {
        //        e.printStackTrace();//TODO
        //    }
        //});
        this.recordCount.getAndIncrement();
    }

    public void build() throws IOException, InterruptedException {
        this.kvWriter.finish();
        populateStatisticsAndWrite();

        GOVMinimalPerfectHashFunctionModified<byte[]> hashFunction = buildHash();
        buildIndex(hashFunction);
    }

    public GOVMinimalPerfectHashFunctionModified<byte[]> buildHash() throws IOException {
        GOVMinimalPerfectHashFunctionModified.Builder<byte[]> builder = new GOVMinimalPerfectHashFunctionModified.Builder<>();
        GOVMinimalPerfectHashFunctionModified<byte[]> hashFunction = builder.store(keys).signed(checksumBits).build();
        keys.close();
        BinIO.storeObject(hashFunction, new File(basePath, FILE_NAME_KEY_HASH));
        return hashFunction;
    }

    public void buildIndex(GOVMinimalPerfectHashFunctionModified<byte[]> hashFunction) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();

        long idxSize = this.recordCount.get();
        //perform multiple pass to build in-memory index using limited memory to void low random disk io
        final long passSize = Math.min(idxSize, this.passCacheSize / SLOT_SIZE);
        long passes = (idxSize / passSize);
        long lastPassSize = passSize;
        if (idxSize % passSize != 0) {
            lastPassSize = idxSize - passes * passSize;
            passes++;
        }
        logger.trace("Generating index, pass size:{}, total pass {}", passSize, passes);

        LBuffer buf = new LBuffer(passSize * SLOT_SIZE);
        LBuffer aBuf = approximateMode ? new LBuffer(passSize * SLOT_SIZE) : null;
        long currentBase = 0;
        try (FileOutputStream fos = new FileOutputStream(new File(basePath, FILE_NAME_KV_INDEX), false);
             FileChannel channel = fos.getChannel();
             FileOutputStream afos = new FileOutputStream(new File(basePath, FILE_NAME_KV_APPROXIMATE_INDEX), false);
             FileChannel aChannel = approximateMode ? afos.getChannel() : null
        ) {
            for (int i = 0; i < passes; i++) {
                //long pStart = System.currentTimeMillis();
                final long rangeStart = currentBase;
                final long currentPasSize = (i == lastPassSize) ? lastPassSize : passSize;
                currentBase += passSize;
                this.kvWriter.forEach((addr, key, value) -> {
                    long idx = hashFunction.getLong(key);
                    idx = idx - rangeStart;
                    if (idx >= 0 && idx < currentPasSize) {
                        if (REVERSE_ORDER) addr = Long.reverseBytes(addr);
                        buf.putLong(idx * SLOT_SIZE, addr);
                        if (approximateMode) {
                            aBuf.readFrom(value, 0, idx * SLOT_SIZE, Math.min(value.length, 8));
                        }

                    }
                });

                long limit = (i == passes - 1 ? lastPassSize : passSize) * SLOT_SIZE;
                writeLBuffer(channel, buf, limit);
                if (approximateMode) writeLBuffer(aChannel, aBuf, limit);
            }
            buf.release();
        }

        logger.trace("generate idx cost:{}", (System.currentTimeMillis() - start));
    }

    private void populateStatisticsAndWrite() {
        this.kvWriter.getStatistics().writeTo(config);
        try {
            configBuilder.save();
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    private long writeLBuffer(FileChannel channel, LBufferAPI buf, long limit) throws IOException {
        int writeOutBatchSize = (int) Math.min(128 * 1024 * 1024, buf.size());
        long offset = 0;
        long writeOut = 0;
        while (offset < limit) {
            long remain = limit - offset;
            int size = remain > writeOutBatchSize ? writeOutBatchSize : (int) remain;
            ByteBuffer bb = buf.toDirectByteBuffer(offset, size);
            offset += size;
            writeOut += channel.write(bb);
        }

        return writeOut;
    }
}