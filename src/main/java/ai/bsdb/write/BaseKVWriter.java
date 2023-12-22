package ai.bsdb.write;

import ai.bsdb.util.Common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class BaseKVWriter implements KVWriter {
    private CopyOnWriteArrayList<byte[]> keySamples = new CopyOnWriteArrayList<>();
    private CopyOnWriteArrayList<byte[]> valueSamples = new CopyOnWriteArrayList<>();
    protected Map<Integer, Double> keyLenPercentile;
    protected Map<Integer, Double> valueLenPercentile;

    protected Statistics statistics = new Statistics();

    @Override
    public void sample(byte[] key, byte[] value) {
        if(key != null && value != null) {
            keySamples.add(key);
            valueSamples.add(value);
        }
    }

    @Override
    public void onSampleFinished() {
        //if(keySamples.size() > 0) keyLenPercentile = percentiles().indexes(0, 50, 75, 90, 95, 99, 100).compute(keySamples.stream().map(key -> key == null ? 0 : key.length).collect(Collectors.toList()));
        //if(valueSamples.size() > 0) valueLenPercentile = percentiles().indexes(0, 50, 75, 90, 95, 99, 100).compute(valueSamples.stream().map(key -> key == null ? 0 : key.length).collect(Collectors.toList()));
        sample0(keySamples, valueSamples);
        keySamples = null;
        valueSamples = null;
    }

    protected int getRecordLength(byte[] key, byte[] value) {
        return getRecordLength(key.length, value.length);
    }

    protected int getRecordLength(int keyLen, int valueLen) {
        return keyLen + valueLen + Common.RECORD_HEADER_SIZE;
    }

    protected void writeRecord2Buffer(byte[] key, byte[] value, ByteBuffer buf) {
        buf.put((byte) key.length);
        buf.putShort((short) value.length);
        buf.put(key);
        buf.put(value);
    }

    protected int readKeyLength(ByteBuffer buf){
        return buf.get() & 0xff;
    }

    protected int readValueLength(ByteBuffer buf){
        return buf.getShort() & 0xFFFF;
    }

    protected void sample0(List<byte[]> keys, List<byte[]> values) {

    }

    public final void put(byte[] key, byte[] value) throws IOException {
        statistics.onRecord(key.length, value.length);
        put0(key, value);
    }

    protected abstract void put0(byte[] key, byte[] value) throws IOException;

    @Override
    public Statistics getStatistics() {
        return this.statistics;
    }
}
