package tech.bsdb.bench;

import tech.bsdb.read.AsyncReader;
import tech.bsdb.serde.ParquetSer;
import tech.bsdb.util.Common;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class AsyncParquetQueryBench {
    static Logger logger = LoggerFactory.getLogger(AsyncParquetQueryBench.class);
    
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
        String keyFieldName = cmd.getOptionValue("kf", null);
        boolean approximate = cmd.hasOption('a');
        boolean verify = cmd.hasOption('v');

        Path inPath = new Path(keyFile);
        Configuration config = new Configuration();
        String nameNodeUrl = cmd.getOptionValue("nn");
        if (!Strings.isNullOrEmpty(nameNodeUrl)) config.set("fs.defaultFS", nameNodeUrl);
        FileSystem fs = FileSystem.get(config);

        FileStatus status = fs.getFileStatus(inPath);

        if (status == null) {
            logger.error("input file not exist: {}", keyFile);
            System.exit(1);
        }

        List<FileStatus> files = new ArrayList<>();
        if (status.isDirectory()) {
            for (FileStatus sub : fs.listStatus(inPath)) {
                if (!sub.isDirectory() && sub.getPath().getName().endsWith(".parquet")) {
                    files.add(sub);
                }
            }
        } else {
            files.add(status);
        }

        if (files.isEmpty()) {
            logger.error("no valid input file found.");
            System.exit(1);
        }

        LongAdder submits = new LongAdder();
        LongAdder successCount = new LongAdder();
        LongAdder failedCount = new LongAdder();

        AtomicBoolean finished = new AtomicBoolean(false);

        AsyncReader db = new AsyncReader(new File(dbPath), approximate, indexDirect, kvDirect);
        final RateLimiter rateLimiter = RateLimiter.create(460 * 1000);
        new Thread(() -> {

            int printInterval = 1;
            while (!finished.get()) {
                try {
                    long last = successCount.sum();
                    Thread.sleep(printInterval * 1000);
                    long rate = (successCount.sum() - last) / printInterval;
                    rateLimiter.setRate((rate + 1) * 1.5);
                    logger.info("submit:{}, finished {}, handled {} queries per seconds, failed {}", submits.sum(), successCount.sum(), rate, failedCount.sum());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }).start();

        ParquetSer parquetReader = new ParquetSer();
        Common.runParallel(Integer.parseInt(System.getProperty("bsdb.query.read.threads", "8")), (pool, futures) -> {
            for (FileStatus file : files) {
                futures.add(pool.submit(() -> {
                    try {
                        parquetReader.read(file, keyFieldName, (key, value) -> {
                            try {
                                if (key.length > Common.MAX_KEY_SIZE | value.length > Common.MAX_VALUE_SIZE) {
                                    logger.warn("record too large, dropped");
                                    return;
                                }
                                db.asyncGet(key, key, new CompletionHandler<byte[], Object>() {
                                    @Override
                                    public void completed(byte[] v, Object object) {
                                        successCount.increment();
                                        if (verify) {
                                            if (v == null) {
                                                logger.error("should not return null");
                                            } else if (!(approximate ? matches(v, value) : Common.bytesEquals(value, v))) {
                                                logger.error("not match: {}->{}, get:{}", new String(key), new String(value), new String(v));
                                            }
                                        }
                                    }

                                    @Override
                                    public void failed(Throwable throwable, Object objects) {
                                        failedCount.increment();
                                    }
                                });
                                submits.increment();
                            } catch (Throwable e) {
                                e.printStackTrace();
                                throw new RuntimeException("error when inserting record.", e);
                            }
                        });


                    } catch (Throwable e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }));
            }
        }, true);

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
        options.addOption("d", "dir", true, "Specify data directory, default to ./rdb");
        options.addOption("ic", "cache-index", false, "Hold index in memory");
        //options.addOption("z", "compressed", false, "DB records compressed");
        options.addOption("v", "verify", false, "verify value matches");
        //options.addOption("bs", "compress-block-size", true, "Specify compress block size, default to 1024");
        options.addOption("a", "approximate", false, "Approximate mode, keys will not be stored, choosing proper checksum bits to meet false-positive query rate");
        options.addOption("id", "index-direct-io", false, "use o_direct to read index");
        options.addOption("kd", "kv-direct-io", false, "use o_direct to read kv");
        options.addOption("nn", "name-node", true, "name node url, e.g. hdfs://localhost:9000");
        options.addOption("kf", "key-name", true, "Specify parquet field name which will use as for database key");
        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            logger.error("Error parsing input args: " + e.getMessage());
            System.exit(1);
            return null;
        }
    }
}
