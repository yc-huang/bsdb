package tech.bsdb.read;

import org.apache.commons.configuration2.Configuration;
import tech.bsdb.io.Native;
import tech.bsdb.read.index.AsyncDirectIndexReader;
import tech.bsdb.read.index.AsyncIndexReader;
import tech.bsdb.read.index.AsyncLBufferIndexReader;
import tech.bsdb.read.kv.BlockedKVReader;
import tech.bsdb.read.kv.CompactKVReader;
import tech.bsdb.read.kv.CompressedKVReader;
import tech.bsdb.read.kv.KVReader;
import tech.bsdb.serde.Field;
import tech.bsdb.util.Common;
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

public class AsyncReader extends Reader {
    private KVReader kvReader;
    private final AsyncIndexReader idxReader;
    Logger logger = LoggerFactory.getLogger(AsyncReader.class);

    public AsyncReader(File basePath, boolean approximate, boolean indexReadDirect, boolean kvReadDirect) throws IOException, ClassNotFoundException {
        super(basePath, approximate);

        boolean useUring = Common.isUringEnabled();
        int submitThreads = Common.getPropertyAsInt("bsdb.reader.index.submit.threads", Math.max(Common.CPUS / 2, 2));
        //int callbackThreads = Common.getPropertyAsInt("bsdb.reader.index.callback.threads", 1);

        this.idxReader = indexReadDirect ? new AsyncDirectIndexReader(idxFile, submitThreads, useUring) : new AsyncLBufferIndexReader(idxFile);
        this.idxCapacity = idxReader.size();
        logger.info("idx capacity:{}", idxCapacity);

        if (!approximate) {
            Configuration config = getConfig();
            this.kvReader = isCompressed() ? new CompressedKVReader(kvFile, config, true, kvReadDirect)
                    : isCompact() ? new CompactKVReader(kvFile, config, true, kvReadDirect) : new BlockedKVReader(kvFile, config, true, kvReadDirect);
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
