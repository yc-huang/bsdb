package ai.bsdb.write;

import ai.bsdb.BaseTest;
import ai.bsdb.read.SyncReader;
import ai.bsdb.util.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class BSDBWriterTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(BSDBWriterTest.class);


    public void testCompactDatabaseBuildAndRead() throws Exception {
        runBuildAndRead(true, false);
    }

    public void testBlockedDatabaseBuildAndRead() throws Exception {
        runBuildAndRead(false, false);
    }

    public void testCompressedDatabaseBuildAndRead() throws Exception {
        runBuildAndRead(false, true);
    }

    public void runBuildAndRead(boolean compact, boolean compress) throws Exception {
        logger.info("build database with compress:{}, compact:{}", compress, compact);
        long start = System.currentTimeMillis();
        BSDBWriter writer = new BSDBWriter(basePath, null, 0, 100 * 1024 * 1024, compact, compress, 4096, 1024 * 1024, false);

        int maxValueLen = Common.MAX_VALUE_SIZE;
        AtomicLong recId = new AtomicLong();

        Common.runParallel(8, new Common.ParallelTask() {
            @Override
            public void addTasks(ExecutorService pool, List<Future> futures) {
                for (int i = 1; i <= maxValueLen; i++) {
                    byte[] value = genValue(i);

                    futures.add(pool.submit(() -> {
                        for (int j = 1; j <= 100; j++) {
                            byte[] key = getKey(recId.incrementAndGet());
                            writer.sample(key, value);
                        }
                    }));


                }
            }
        }, true);
        writer.onSampleFinished();

        recId.set(0);

        Common.runParallel(8, new Common.ParallelTask() {
            @Override
            public void addTasks(ExecutorService pool, List<Future> futures) {
                for (int i = 1; i <= maxValueLen; i++) {
                    byte[] value = genValue(i);
                    for (int j = 1; j <= Common.MAX_KEY_SIZE; j++) {
                        byte[] key = getKey(recId.incrementAndGet());
                        futures.add(pool.submit(() -> {
                            try {
                                writer.put(key, value);
                            } catch (IOException | InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }));

                    }
                }
            }
        }, true);
        long recCount = recId.get();
        logger.info("put {} records cost {}", recCount, System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        writer.build();
        logger.info("build hash and index cost {}", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        SyncReader reader = new SyncReader(basePath, false, false, false, false);
        query(reader, maxValueLen);

        logger.info("query {} existing records using {index direct: false, kv direct: false} cost {}", recCount, System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        for (int i = 0; i < 10000; i++) {
            byte[] key = getKey(++recCount);
            byte[] value = reader.getAsBytes(key);
            assertNull(value);
        }
        logger.info("query 10000 non-existing records using {index direct: false, kv direct: false} cost {}", System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        reader = new SyncReader(basePath, false, false, true, false);
        query(reader, maxValueLen);
        logger.info("query {} existing records using {index direct: true, kv direct: false} cost {}", recCount, System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            byte[] key = getKey(++recCount);
            byte[] value = reader.getAsBytes(key);
            assertNull(value);
        }
        logger.info("query 10000 non-existing records using {index direct: true, kv direct: false} cost {}", System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        reader = new SyncReader(basePath, false, false, false, true);
        query(reader, maxValueLen);
        logger.info("query {} existing records using {index direct: false, kv direct: true} cost {}", recCount, System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            byte[] key = getKey(++recCount);
            byte[] value = reader.getAsBytes(key);
            assertNull(value);
        }
        logger.info("query 10000 non-existing records using {index direct: false, kv direct: true} cost {}", System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        reader = new SyncReader(basePath, false, false, true, true);
        query(reader, maxValueLen);
        logger.info("query {} existing records using {index direct: true, kv direct: true} cost {}", recCount, System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            byte[] key = getKey(++recCount);
            byte[] value = reader.getAsBytes(key);
            assertNull(value);
        }
        logger.info("query 10000 non-existing records using {index direct: true, kv direct: true} cost {}", System.currentTimeMillis() - start);
    }

    private void query(SyncReader reader, int maxValueLen) {
        AtomicLong recCount = new AtomicLong();
        Common.runParallel(8, new Common.ParallelTask() {
            @Override
            public void addTasks(ExecutorService pool, List<Future> futures) {
                for (int i = 1; i <= maxValueLen; i++) {
                    for (int j = 1; j <= Common.MAX_KEY_SIZE; j++) {
                        byte[] key = getKey(recCount.incrementAndGet());
                        final int vLen = i;
                        futures.add(pool.submit(() -> {
                            byte[] value = new byte[0];
                            try {
                                value = reader.getAsBytes(key);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            assertNotNull(value);
                            assertEquals(vLen, value.length);
                        }));

                    }
                }
            }
        }, true);
    }

    byte[] getKey(long index) {
        return ("" + index).getBytes();
    }
}