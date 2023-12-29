package tech.bsdb.read;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunctionModified;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import tech.bsdb.read.kv.KVReader;
import tech.bsdb.serde.Field;
import tech.bsdb.util.Common;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.util.Objects;

public abstract class Reader {
    protected File idxFile;
    protected File kvFile;
    protected long idxCapacity;
    protected boolean approximateMode;
    protected GOVMinimalPerfectHashFunctionModified<byte[]> hashFunction;
    private Configuration config;
    private Field[] valueSchema;

    public Reader(File basePath, boolean approximate) throws IOException, ClassNotFoundException {
        this.hashFunction = (GOVMinimalPerfectHashFunctionModified<byte[]>) BinIO.loadObject(new File(basePath, Common.FILE_NAME_KEY_HASH));
        this.approximateMode = approximate;
        this.idxFile = new File(basePath, approximate ? Common.FILE_NAME_KV_APPROXIMATE_INDEX : Common.FILE_NAME_KV_INDEX);
        this.kvFile = new File(basePath, Common.FILE_NAME_KV_DATA);

        try {
            this.config = new Configurations().properties(new File(basePath, Common.FILE_NAME_CONFIG));
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }

        File schemaFile = new File(basePath, Common.FILE_NAME_VALUE_SCHEMA);
        if (schemaFile.exists()) {
            this.valueSchema = (Field[]) BinIO.loadObject(schemaFile);
        }
    }

    public Field[] getValueSchema() {
        return this.valueSchema;
    }

    public Configuration getConfig(){
        return this.config;
    }

    public boolean isCompressed(){
        return config.getBoolean(Common.CONFIG_KEY_KV_COMPRESS);
    }

    public boolean isCompact(){
        return config.getBoolean(Common.CONFIG_KEY_KV_COMPACT);
    }

    protected long checkAndGetIndex(byte[] key) {
        if (!Objects.isNull(key)) {
            return hashFunction.getLong(key);
        } else {
            return -1;
        }
    }

    public interface IndexCompletionHandler {
        public void completed(Long addr, KVReader reader, byte[] key, CompletionHandler<byte[], Object> appHandler, Object appAttach);
        public void failed(Throwable throwable, CompletionHandler<byte[], Object> appHandler, Object appAttach);
    }
}
