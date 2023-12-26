package tech.bsdb.read;

import tech.bsdb.read.kv.KVReader;
import tech.bsdb.serde.Field;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import org.apache.commons.configuration2.Configuration;

import java.nio.channels.CompletionHandler;
import java.util.Objects;

public abstract class Reader {
    protected long idxCapacity;
    protected boolean approximateMode;
    protected GOVMinimalPerfectHashFunction<byte[]> hashFunction;
    protected Configuration config;
    protected Field[] valueSchema;

    public Field[] getValueSchema() {
        return this.valueSchema;
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
