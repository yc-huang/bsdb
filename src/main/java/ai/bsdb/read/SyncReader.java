package ai.bsdb.read;

import ai.bsdb.read.index.DirectIndexReader;
import ai.bsdb.read.index.IndexReader;
import ai.bsdb.read.index.LBufferIndexReader;
import ai.bsdb.read.kv.BlockedKVReader;
import ai.bsdb.read.kv.CompactKVReader;
import ai.bsdb.read.kv.CompressedKVReader;
import ai.bsdb.read.kv.KVReader;
import ai.bsdb.serde.Field;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;

import static ai.bsdb.util.Common.*;

public class SyncReader extends Reader {
    private KVReader kvReader;
    private final IndexReader idxReader;
    Logger logger = LoggerFactory.getLogger(SyncReader.class);

    public SyncReader(File basePath, boolean loadIndex2Mem, boolean approximate, boolean indexReadDirect, boolean kvReadDirect) throws IOException, ClassNotFoundException {
        this.hashFunction = (GOVMinimalPerfectHashFunction<byte[]>) BinIO.loadObject(new File(basePath, FILE_NAME_KEY_HASH));
        File idxFile = new File(basePath, approximate ? FILE_NAME_KV_APPROXIMATE_INDEX : FILE_NAME_KV_INDEX);
        this.idxReader = indexReadDirect ? new DirectIndexReader(idxFile) : new LBufferIndexReader(idxFile, loadIndex2Mem);
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
