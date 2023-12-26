package tech.bsdb.write;

import tech.bsdb.BaseTest;
import tech.bsdb.util.Common;

import java.io.File;
import java.io.IOException;

public class KVWriterCompressedTest extends BaseTest {
    /**
     * This test class tests the functionality of the KVWriterCompressed class.
     * It generates sample key-value pairs using the gen() method and writes them to a file using the sample() method of the KVWriterCompressed class.
     * Then, it generates more key-value pairs using the same gen() method and writes them to the same file using the put() method of the KVWriterCompressed class.
     * Finally, it reads all the records from the file using the forEach() method and compares the number of records found with the expected number of records.
     */
    public void testSample() throws IOException {
        System.gc();
        long bf = Runtime.getRuntime().freeMemory();
        KVWriterCompressed writer = new KVWriterCompressed(new File(basePath, Common.FILE_NAME_KV_DATA), 4096, 1024 * 1024, true);
        System.gc();
        System.err.println(Runtime.getRuntime().totalMemory() + "  " + Runtime.getRuntime().freeMemory() + "  " + (bf - Runtime.getRuntime().freeMemory()));
        try {
            for (int i = 10; i <= Common.MAX_VALUE_SIZE; i++) {
                byte[] value = gen(i);
                for (int j = 1; j <= 2; j++) {
                    byte[] key = gen(j);
                    writer.sample(key, value);
                }
            }
            writer.onSampleFinished();
        } catch (Throwable t) {

        }

        int recCount = 0;
        for (int i = 10; i <= Common.MAX_VALUE_SIZE; i++) {
            byte[] value = gen(i);
            for (int j = 1; j <= Common.MAX_KEY_SIZE; j++) {
                byte[] key = gen(j);
                writer.put(key, value);
                recCount++;
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