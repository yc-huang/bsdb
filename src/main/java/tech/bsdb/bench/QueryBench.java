package tech.bsdb.bench;

import tech.bsdb.read.SyncReader;
import tech.bsdb.util.Common;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class QueryBench {
    static Logger logger = LoggerFactory.getLogger(QueryBench.class);

    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);

        if (!cmd.hasOption("k")) {
            logger.error("Must specify file contains query keys.");
            System.exit(1);
        }

        String dbPath = cmd.getOptionValue("d", "./rdb");
        boolean indexDirect = cmd.hasOption("id");
        boolean kvDirect = cmd.hasOption("kd");

        String keyFile = cmd.getOptionValue("k");
        if (Objects.isNull(keyFile)) {
            logger.error("must specify key file.");
            System.exit(1);
        }

        File inputPath = new File(keyFile);
        if (!inputPath.exists()) {
            logger.error("key file not exist:{}", inputPath);
            System.exit(1);
        }

        String separator = cmd.getOptionValue("s", " ");
        boolean approximate = cmd.hasOption('a');
        boolean verify = cmd.hasOption('v');

        File[] inputFiles = new File[]{inputPath};
        if (inputPath.isDirectory()) {
            inputFiles = inputPath.listFiles(
                    file -> !file.isDirectory() && (file.getName().endsWith(".txt") | file.getName().endsWith(".gz") | file.getName().endsWith(".zstd"))
            );
        }

        if (inputFiles == null || inputFiles.length == 0) {
            logger.error("no valid key file found.");
            System.exit(1);
        }

        SyncReader db = new SyncReader(new File(dbPath), cmd.hasOption("ic"), approximate, indexDirect, kvDirect);

        long start = System.currentTimeMillis();
        LongAdder count = new LongAdder();
        AtomicBoolean finished = new AtomicBoolean(false);
        LongAdder queryCount = new LongAdder();
        final RateLimiter rateLimiter = RateLimiter.create(500 * 1000);
        new Thread(() -> {
            int printInterval = 1;
            while (!finished.get()) {
                try {long last = queryCount.sum();
                    Thread.sleep(printInterval * 1000);
                    long processed = queryCount.sum() - last;
                    logger.info("handled {} queries per seconds, free mem:{}MB", processed / printInterval, Runtime.getRuntime().freeMemory()/1024/1024);
                    long rate = processed / printInterval;
                    //rateLimiter.setRate(Math.max(rate, 10000));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }).start();

        File[] finalInputFiles = inputFiles;

        //int submitThreads = Integer.parseInt(System.getProperty("bsdb.query.submit.threads", "32"));
        //ExecutorService executor = new ThreadPoolExecutor(submitThreads, submitThreads, 1, TimeUnit.MINUTES, new DisruptorBlockingQueueModified<>(8192));

        for (File input : finalInputFiles) {
            try (BufferedReader reader = Common.getReader(input, StandardCharsets.UTF_8)) {
                //Caution: parallel stream might buffer lots of items, which could cause low memory. only for short-run benchmark test
                reader.lines().parallel().forEach(line ->  {
                    count.increment();
                    if(Runtime.getRuntime().maxMemory() > Runtime.getRuntime().freeMemory() * 10) rateLimiter.acquire();//apply rate control when memory low
                    String[] cols = line.split(separator);
                    if (cols.length == 2) {
                        byte[] key = cols[0].getBytes();
                        byte[] value = cols[1].getBytes();
                        if (key.length > Common.MAX_KEY_SIZE | value.length > Common.MAX_VALUE_SIZE) {
                            logger.error("record too large, dropped: {}", Arrays.toString(cols));
                            return;
                        }
                        //ByteBuffer v = db.get(key);
                        byte[] v = new byte[0];
                        try {
                            v = db.getAsBytes(key);
                        } catch (Exception e) {
                            logger.error("query failed", e);
                            throw new RuntimeException(e);
                        }
                        if (verify) {
                            if (v == null) {
                                logger.error("should not return null for :{}->{}", new String(key), new String(value));
                            } else if (!(approximate ? matches(v, value) : Common.bytesEquals(value, v))) {
                                //System.err.println("not match:" + new String(key) + "," + new String(value) + "," + v.asCharBuffer().toString());
                                logger.error("not match:{}->{}, get:{}", new String(key), new String(value), new String(v));
                            }
                        }
                        queryCount.increment();
                    }

                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        logger.info("Query {} existing keys cost {} ms", count.sum(), (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        long iteration = 50 * 1000 * 1000;
        Random random = new Random();

        //testing non-existing keys
        random.longs(iteration).parallel().mapToObj(l -> String.valueOf(Math.abs(l)).getBytes()).forEach(k -> {
            try {
                byte[] v = db.getAsBytes(k);
                assert v == null;
                queryCount.increment();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        });

        logger.info("query {} non-existing keys cost: {}ms", iteration, (System.currentTimeMillis() - start));

        finished.set(true);
    }

    static boolean matches(byte[] out, byte[] in) {
        int maxLen = Math.min(in.length, 8);
        for (int i = 0; i < maxLen; i++) {
            if (in[i] != out[i]) return false;
        }
        return true;
    }

    private static CommandLine parseArgs(String[] args) {
        Options options = new Options();
        options.addOption("k", "keys", true, "Specify input file for sequential query keys");
        options.addOption("s", "separator", true, "Specify key/value separator, default to space\" \"");
        options.addOption("d", "dir", true, "Specify data directory, default to ./rdb");
        options.addOption("ic", "cache-index", false, "Hold index in memory");
        //options.addOption("z", "compressed", false, "DB records compressed");
        options.addOption("v", "verify", false, "verify value matches");
        //options.addOption("bs", "compress-block-size", true, "Specify compress block size, default to 1024");
        options.addOption("a", "approximate", false, "Approximate mode, keys will not be stored, choosing proper checksum bits to meet false-positive query rate");
        options.addOption("id", "index-direct-io", false, "use o_direct to read index");
        options.addOption("kd", "kv-direct-io", false, "use o_direct to read kv");
        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            logger.error("Error parsing input args: {}", e.getMessage());
            System.exit(1);
            return null;
        }
    }
}
