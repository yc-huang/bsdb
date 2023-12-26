package tech.bsdb.write;

import tech.bsdb.BaseTest;
import tech.bsdb.util.Common;

import java.io.File;
import java.io.IOException;

public class SimpleBlockedKVWriterTest extends BaseTest {
    SimpleBlockedKVWriter writer;

    public void setUp() throws Exception {
        super.setUp();
        basePath.mkdir();
    }

    public void tearDown() throws Exception {
        basePath.deleteOnExit();
    }

    public void testPut() throws IOException {
        writer = new SimpleBlockedKVWriter(new File(basePath, Common.FILE_NAME_KV_DATA), 4096,8);
        int recCount = 0;
        for(int i = 1; i <= Common.MAX_VALUE_SIZE; i++){
            byte[] value = gen(i);
            for(int j = 1; j <= Common.MAX_KEY_SIZE; j++){
              byte[] key = gen(j);
              writer.put(key, value);
              recCount ++;
            }
        }
        writer.finish();

        long found = writer.forEach(new KVWriter.ScanHandler() {
            @Override
            public void handleRecord(long addr, byte[] key, byte[] value) {

            }
        });

        assertEquals(recCount, found);
    }
}