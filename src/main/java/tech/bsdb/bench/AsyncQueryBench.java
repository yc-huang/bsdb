package tech.bsdb.bench;

import tech.bsdb.read.AsyncReader;
import tech.bsdb.util.Common;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class AsyncQueryBench {
    static Logger logger = LoggerFactory.getLogger(AsyncQueryBench.class);
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
            logger.error("key file not exist: {}", inputPath);
            System.exit(1);
        }

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

        String separator = cmd.getOptionValue("s", " ");

        boolean approximate = cmd.hasOption('a');
        boolean verify = cmd.hasOption('v');

        long start = System.currentTimeMillis();

        LongAdder submits = new LongAdder();
        LongAdder successCount = new LongAdder();
        LongAdder failedCount = new LongAdder();

        AtomicBoolean finished = new AtomicBoolean(false);
        AsyncReader db = new AsyncReader(new File(dbPath), approximate, indexDirect, kvDirect);
        //final RateLimiter rateLimiter = RateLimiter.create(460 * 1000);
        new Thread(() -> {

            int printInterval = 1;
            while (!finished.get()) {
                try {
                    long last = successCount.sum();
                    Thread.sleep(printInterval * 1000);
                    long rate = (successCount.sum() - last) / printInterval;
                    //rateLimiter.setRate((rate + 1) * 1.5);
                    logger.info("submit:{}, finished {}, handled {} queries per seconds, failed {}", submits.sum(), successCount.sum(), rate, failedCount.sum());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }).start();


        File[] finalInputFiles = inputFiles;
        //Common.runParallel(Integer.parseInt(System.getProperty("bsdb.query.read.threads", "2")), (pool, futures) -> {
            for (File input : finalInputFiles) {
          //      futures.add(pool.submit(() -> {
                    try (BufferedReader reader = Common.getReader(input, StandardCharsets.UTF_8)) {
                        reader.lines().parallel().map(line -> line.split(separator)).forEach(cols -> {

                            if (cols.length == 2) {
                                final byte[] key = cols[0].getBytes();
                                final byte[] value = cols[1].getBytes();

                                //rateLimiter.acquire();

                                try {
                                    db.asyncGet(key, key, new CompletionHandler<byte[], Object>() {
                                        @Override
                                        public void completed(byte[] v, Object object) {
                                            successCount.increment();
                                            if (verify) {
                                                if (v == null) {
                                                    logger.error("should not return null for key:{}", Arrays.toString(key));
                                                } else if (!(approximate ? matches(v, value) : Common.bytesEquals(value, v))) {
                                                    //System.err.println("not match:" + new String(key) + "," + new String(value) + "," + v.asCharBuffer().toString());
                                                    logger.error("not match: {}->{}, get:{}", new String(key), new String(value), new String(v));
                                                }
                                            }
                                        }

                                        @Override
                                        public void failed(Throwable throwable, Object objects) {
                                            logger.error("query failed", throwable);
                                            failedCount.increment();
                                        }
                                    });
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                submits.increment();
                            }
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
            //    }));

            }
        //}, true);

        while (successCount.sum() + failedCount.sum() != submits.sum()) {
            Thread.sleep(1000);
        }
        finished.set(true);
        System.exit(0);
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
