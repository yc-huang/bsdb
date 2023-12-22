package ai.bsdb.read;

import ai.bsdb.read.index.AsyncDirectIndexReader;
import ai.bsdb.read.index.AsyncIndexReader;
import ai.bsdb.read.index.AsyncLBufferIndexReader;
import ai.bsdb.read.kv.BlockedKVReader;
import ai.bsdb.read.kv.CompactKVReader;
import ai.bsdb.read.kv.CompressedKVReader;
import ai.bsdb.read.kv.KVReader;
import ai.bsdb.serde.Field;
import ai.bsdb.util.Common;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;

import static ai.bsdb.util.Common.*;

public class AsyncReader extends Reader {
    private KVReader kvReader;
    private final AsyncIndexReader idxReader;
    Logger logger = LoggerFactory.getLogger(AsyncReader.class);

    public AsyncReader(File basePath, boolean approximate, boolean indexReadDirect, boolean kvReadDirect) throws IOException, ClassNotFoundException {
        this.hashFunction = (GOVMinimalPerfectHashFunction<byte[]>) BinIO.loadObject(new File(basePath, FILE_NAME_KEY_HASH));

        boolean useUring = Boolean.getBoolean("bsdb.uring");
        int submitThreads = Common.getPropertyAsInt("bsdb.reader.index.submit.threads", Math.max(Common.CPUS / 2, 2));
        //int callbackThreads = Common.getPropertyAsInt("bsdb.reader.index.callback.threads", 1);

        File idxFile = new File(basePath, approximate ? FILE_NAME_KV_APPROXIMATE_INDEX : FILE_NAME_KV_INDEX);
        this.idxReader = indexReadDirect ? new AsyncDirectIndexReader(idxFile, submitThreads, useUring) : new AsyncLBufferIndexReader(idxFile);
        this.idxCapacity = idxReader.size();
        logger.info("idx capacity:{}", idxCapacity);
        this.approximateMode = approximate;

        try {
            config = new Configurations().properties(new File(basePath, FILE_NAME_CONFIG));
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }

        File schemaFile = new File(basePath, FILE_NAME_VALUE_SCHEMA);
        if (schemaFile.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(schemaFile.toPath()))) {
                valueSchema = (Field[]) ois.readObject();
            }
        }

        if (!approximate) {
            File kvFile = new File(basePath, FILE_NAME_KV_DATA);
            boolean compress = config.getBoolean(CONFIG_KEY_KV_COMPRESS);
            boolean compact = config.getBoolean(CONFIG_KEY_KV_COMPACT);
            this.kvReader = compress ? new CompressedKVReader(kvFile, config, true, kvReadDirect)
                    : compact ? new CompactKVReader(kvFile, config, true, kvReadDirect) : new BlockedKVReader(kvFile, config, true, kvReadDirect);
        }
    }

    public boolean asyncGet(byte[] key, final Object attach1, final CompletionHandler<byte[], Object> handler) throws InterruptedException {
        long index = checkAndGetIndex(key);
        if (index != -1 && index < idxCapacity) {
            return idxReader.asyncGetAddrAt(index, kvReader, key, handler, attach1, new IndexCompletionHandler() {
                @Override
                public void completed(Long addr, KVReader reader, byte[] key, CompletionHandler<byte[], Object> appHandler, Object appAttach) {
                    if (approximateMode) appHandler.completed(key, appAttach);
                    else {
                        if (addr >= 0) {
                            try {
                                reader.asyncGetValueAsBytes(addr, key, new CompletionHandler<>() {
                                    @Override
                                    public void completed(byte[] value, byte[] k) {
                                        appHandler.completed(value, appAttach);
                                    }

                                    @Override
                                    public void failed(Throwable throwable, byte[] k) {
                                        appHandler.failed(throwable, appAttach);
                                    }
                                });
                            } catch (Exception e) {
                                logger.error(reader.getClass() + ".asyncGetValueAsBytes(" + addr + ") failed.", e);
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }

                @Override
                public void failed(Throwable throwable, CompletionHandler<byte[], Object> appHandler, Object appAttach) {
                    appHandler.failed(throwable, appAttach);
                }
            });
        } else {
            return false;
        }
    }


}
