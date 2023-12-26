package tech.bsdb.write;

import tech.bsdb.util.Common;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

public interface KVWriter {
    public void sample(byte[] key, byte[] value);
    public void onSampleFinished();
    void put(byte[] key, byte[] value) throws IOException;
    void finish() throws IOException;
    long forEach(ScanHandler handler) throws IOException, InterruptedException;
    Statistics getStatistics();

    interface ScanHandler {
        void handleRecord(long addr, byte[] key, byte[] value);
    }


    public static class Statistics {
        Item sRecLen = new Item(), sKeyLen = new Item(), sValueLen = new Item(), sCompressedBlockLen = new Item(), sBlockLen = new Item();

        void onRecord(int keyLen, int valueLen) {
            sKeyLen.update(keyLen);
            sValueLen.update(valueLen);
            int recLen = keyLen = valueLen + Common.RECORD_HEADER_SIZE;
            sRecLen.update(recLen);
        }

        public void onBlock(int blockLen, int compressedLen) {
            sBlockLen.update(blockLen);
            sCompressedBlockLen.update(compressedLen);
        }

        public void writeTo(Configuration config){
            config.setProperty(Common.CONFIG_KEY_KV_COUNT, sKeyLen.count());

            config.setProperty(Common.CONFIG_KEY_KV_RECORD_LEN_MAX, sRecLen.max());
            config.setProperty(Common.CONFIG_KEY_KV_RECORD_LEN_AVG, sRecLen.average());

            config.setProperty(Common.CONFIG_KEY_KV_KEY_LEN_MAX, sKeyLen.max());
            config.setProperty(Common.CONFIG_KEY_KV_KEY_LEN_AVG, sKeyLen.average());

            config.setProperty(Common.CONFIG_KEY_KV_VALUE_LEN_MAX, sValueLen.max());
            config.setProperty(Common.CONFIG_KEY_KV_VALUE_LEN_AVG, sValueLen.average());

            config.setProperty(Common.CONFIG_KEY_KV_BLOCK_LEN_MAX, sBlockLen.max());
            config.setProperty(Common.CONFIG_KEY_KV_BLOCK_LEN_AVG, sBlockLen.average());

            config.setProperty(Common.CONFIG_KEY_KV_BLOCK_COMPRESS_LEN_MAX, sCompressedBlockLen.max());
            config.setProperty(Common.CONFIG_KEY_KV_BLOCK_COMPRESS_LEN_AVG, sCompressedBlockLen.average());
        }

        class Item {
            LongAdder total = new LongAdder();
            LongAdder count = new LongAdder();
            LongAccumulator max = new LongAccumulator(Math::max, 0);

            void update(long v) {
                total.add(v);
                count.increment();
                max.accumulate(v);
            }

            long average(){
                long c = count.sum();
                return  c == 0 ? 0 : total.sum() / c;
            }

            long max(){
                return max.get();
            }

            long count(){
                return count.sum();
            }
        }
    }
}
