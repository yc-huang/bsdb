package tech.bsdb.read;

import tech.bsdb.read.index.DirectIndexReader;
import tech.bsdb.read.index.IndexReader;
import tech.bsdb.read.index.LBufferIndexReader;
import tech.bsdb.read.kv.BlockedKVReader;
import tech.bsdb.read.kv.CompactKVReader;
import tech.bsdb.read.kv.CompressedKVReader;
import tech.bsdb.read.kv.KVReader;
import tech.bsdb.serde.Field;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.bsdb.util.Common;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;

public class SyncReader extends Reader {
    private KVReader kvReader;
    private final IndexReader idxReader;
    Logger logger = LoggerFactory.getLogger(SyncReader.class);

    public SyncReader(File basePath, boolean loadIndex2Mem, boolean approximate, boolean indexReadDirect, boolean kvReadDirect) throws IOException, ClassNotFoundException {
        this.hashFunction = (GOVMinimalPerfectHashFunction<byte[]>) BinIO.loadObject(new File(basePath, Common.FILE_NAME_KEY_HASH));
        File idxFile = new File(basePath, approximate ? Common.FILE_NAME_KV_APPROXIMATE_INDEX : Common.FILE_NAME_KV_INDEX);
        this.idxReader = indexReadDirect ? new DirectIndexReader(idxFile) : new LBufferIndexReader(idxFile, loadIndex2Mem);
        this.idxCapacity = idxReader.size();
        logger.info("idx capacity:{}", idxCapacity);
        this.approximateMode = approximate;

        try {
            config = new Configurations().properties(new File(basePath, Common.FILE_NAME_CONFIG));
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }

        File schemaFile = new File(basePath, Common.FILE_NAME_VALUE_SCHEMA);
        if (schemaFile.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(schemaFile.toPath()))) {
                valueSchema = (Field[]) ois.readObject();
            }
        }

        if (!approximate) {
            File kvFile = new File(basePath, Common.FILE_NAME_KV_DATA);
            boolean compress = config.getBoolean(Common.CONFIG_KEY_KV_COMPRESS);
            boolean compact = config.getBoolean(Common.CONFIG_KEY_KV_COMPACT);
            this.kvReader = compress ? new CompressedKVReader(kvFile, config, false, kvReadDirect)
                    : compact ? new CompactKVReader(kvFile, config, false, kvReadDirect) : new BlockedKVReader(kvFile, config, false, kvReadDirect);
        }
    }

    public byte[] getAsBytes(byte[] key) throws Exception {
        long index = checkAndGetIndex(key);
        if (index != -1 && index < idxCapacity) {
            //might exist
            if (approximateMode) {
                return idxReader.getRawBytesAt(index);
            } else {
                long addr = idxReader.getAddrAt(index);
                if (addr >= 0)
                    return kvReader.getValueAsBytes(addr, key);
            }
        }
        return null;
    }
}
