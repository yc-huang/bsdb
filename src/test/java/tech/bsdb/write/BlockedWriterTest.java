package tech.bsdb.write;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

public class BlockedWriterTest extends TestCase {
    ByteBuffer buf;
    public void testPut() throws IOException {

        for(int i = 1; i < 256; i++){
            BlockedKVWriter writer = new BlockedKVWriter(4096, 1) {
                @Override
                void flushBlocks0(int partition, ByteBuffer buffer) throws IOException {
                    buf = buffer;
                }

                @Override
                void finishPartiton0(int partition) throws IOException {

                }

                @Override
                ByteBuffer readBlockAt(int partition, long position) throws IOException {
                    return buf;
                }
            };
            byte[] key = gen(i);
            byte[] value = key;
            writer.put(key, value);
            writer.finish();
            //assertEquals(i + i + Common.RECORD_HEADER_SIZE, buf.position());
            //buf.position(0);
            assertEquals(i, writer.readKeyLength(buf));
            assertEquals(i, writer.readValueLength(buf));

        }

    }



    byte[] gen(int len){
        byte[] buf = new byte[len];
        ThreadLocalRandom.current().nextBytes(buf);
        return buf;
    }
}