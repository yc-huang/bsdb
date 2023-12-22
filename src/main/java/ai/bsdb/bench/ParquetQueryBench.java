package ai.bsdb.bench;

import ai.bsdb.read.SyncReader;
import ai.bsdb.serde.ParquetSer;
import ai.bsdb.util.Common;
import com.google.common.base.Strings;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class ParquetQueryBench {
    static Logger logger = LoggerFactory.getLogger(ParquetQueryBench.class);

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

        SyncReader db = new SyncReader(new File(dbPath), cmd.hasOption("ic"), approximate, indexDirect, kvDirect);

        long start = System.currentTimeMillis();
        LongAdder count = new LongAdder();
        AtomicBoolean finished = new AtomicBoolean(false);
        LongAdder queryCount = new LongAdder();
        new Thread(() -> {
            long last = queryCount.sum();
            int printInterval = 10;
            while (!finished.get()) {
                try {
                    Thread.sleep(printInterval * 1000);
                    logger.info("handled {} queries per seconds", (queryCount.sum() - last) / printInterval);
                    last = queryCount.sum();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }).start();

        ParquetSer parquetReader = new ParquetSer();
        Common.runParallel(Integer.parseInt(System.getProperty("bsdb.query.read.threads", "8")), (pool, futures) -> {
            for (FileStatus file : files) {
                futures.add(pool.submit(() -> {
                    long start0 = System.currentTimeMillis();
                    try {
                        parquetReader.read(file, keyFieldName, (key, value) -> {
                            try {
                                if (key.length > Common.MAX_KEY_SIZE | value.length > Common.MAX_VALUE_SIZE) {
                                    logger.warn("record too large, dropped");
                                    return;
                                }
                                byte[] v;
                                try {
                                    v = db.getAsBytes(key);
                                } catch (Exception e) {
                                    logger.error("query failed", e);
                                    throw new RuntimeException(e);
                                }
                                if (verify) {
                                    if (v == null) {
                                        logger.error("should not return null for {}->{}", new String(key), new String(value));
                                    } else if (!(approximate ? matches(v, value) : Common.bytesEquals(value, v))) {
                                        logger.error("not match: {}->{}, get:{}", new String(key), new String(value), new String(v));
                                    }
                                }
                                queryCount.increment();
                            } catch (Throwable e) {
                                logger.error("query failed", e);
                                throw new RuntimeException("error when inserting record.", e);
                            }
                        });


                    } catch (Throwable e) {
                        logger.error("failed", e);
                        throw new RuntimeException(e);
                    }
                }));
            }
        }, true);

        logger.info("Query {} existing keys cost: {}", count.sum(), (System.currentTimeMillis() - start));

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
        //options.addOption("s", "separator", true, "Specify key/value separator, default to space\" \"");
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
