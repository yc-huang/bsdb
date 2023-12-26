package tech.bsdb.write;

import tech.bsdb.BaseTest;
import tech.bsdb.util.Common;

import java.io.File;
import java.io.IOException;

public class SimpleCompactKVWriterTest extends BaseTest {

    public void testPut() throws IOException {

        SimpleCompactKVWriter writer = new SimpleCompactKVWriter(new File(basePath, Common.FILE_NAME_KV_DATA), 8);
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