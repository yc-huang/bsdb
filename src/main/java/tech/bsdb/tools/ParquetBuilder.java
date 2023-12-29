package tech.bsdb.tools;

import it.unimi.dsi.fastutil.io.BinIO;
import tech.bsdb.read.SyncReader;
import tech.bsdb.serde.Field;
import tech.bsdb.serde.ParquetSer;
import tech.bsdb.util.Common;
import tech.bsdb.write.BSDBWriter;
import com.google.common.base.Strings;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class ParquetBuilder {
    private static final String STORED_ENCODING = "UTF-8";
    private static final String DEFAULT_CHECKSUM_BITS = "4";
    private static final String DEFAULT_PASS_CACHE_SIZE = "1024";//1GB
    private static final String DEFAULT_SAMPLE_COUNT = "10000";
    private static final String DEFAULT_DICT_SIZE = "1";

    static Logger logger = LoggerFactory.getLogger(ParquetBuilder.class);

    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);
        boolean quietMode = cmd.hasOption("q");
        boolean verify = cmd.hasOption("v");
        boolean compact = cmd.hasOption("c");
        boolean compress = cmd.hasOption("z");
        boolean approximateMode = cmd.hasOption("a");

        String inputFile = cmd.getOptionValue("i");
        String keyFieldName = cmd.getOptionValue("kf", null);
        if (Objects.isNull(inputFile)) {
            logger.error("must specify input file.");
            System.exit(1);
        }
        if (Objects.isNull(keyFieldName)) {
            logger.error("must specify parquet field name for database key.");
            System.exit(1);
        }

        String out = cmd.getOptionValue("o", "./rdb/");
        File outPath = new File(out);
        if (outPath.exists()) {
            if (outPath.isFile()) {
                logger.error("output directory exist but not as directory: {}", out);
                System.exit(1);
            }
        } else {
            outPath.mkdirs();
        }

        String tempDir = cmd.getOptionValue("temp", null);
        File tempDirFile = Strings.isNullOrEmpty(tempDir) ? null : new File(tempDir);
        if (tempDirFile != null) {
            if (tempDirFile.exists()) {
                if (tempDirFile.isFile()) {
                    logger.error("temp directory exist but not as directory: {}", tempDir);
                    System.exit(1);
                }
            } else {
                tempDirFile.mkdirs();
            }
        }

        String checksumBitsString = cmd.getOptionValue("cb", DEFAULT_CHECKSUM_BITS);
        int checksumBits = Integer.parseInt(checksumBitsString);
        int sampleCount = Integer.parseInt(cmd.getOptionValue("sc", DEFAULT_SAMPLE_COUNT));
        int dictSize = Integer.parseInt(cmd.getOptionValue("ds", DEFAULT_DICT_SIZE)) * 1024 * 1024;

        long passCacheSize = Long.parseLong(cmd.getOptionValue("ps", DEFAULT_PASS_CACHE_SIZE)) * 1024 * 1024;
        int compressBlockSize = Integer.parseInt(cmd.getOptionValue("bs", "" + Common.DEFAULT_COMPRESS_BLOCK_SIZE));

        AtomicLong readCount = new AtomicLong();
        AtomicLong writeCount = new AtomicLong();

        long start = System.currentTimeMillis();
        BSDBWriter rdbWriter = new BSDBWriter(outPath, tempDirFile, checksumBits, passCacheSize, compact, compress, compressBlockSize, dictSize, approximateMode);

        Path inPath = new Path(inputFile);
        Configuration config = new Configuration();
        String nameNodeUrl = cmd.getOptionValue("nn");
        if (!Strings.isNullOrEmpty(nameNodeUrl)) config.set("fs.defaultFS", nameNodeUrl);
        FileSystem fs = FileSystem.get(config);

        FileStatus status = fs.getFileStatus(inPath);

        if (status == null) {
            System.err.println("input file not exist:" + inputFile);
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
            System.err.println("no valid input file found.");
            System.exit(1);
        }
        ;

        ParquetSer parquetReader = new ParquetSer();
        //save parquet file schema
        Field[] schema = parquetReader.getSchema(files.get(0), keyFieldName);
        BinIO.storeObject(schema, new File(outPath, Common.FILE_NAME_VALUE_SCHEMA));

        int sampledFiles = Math.min(8, files.size()); //no need to sample too much files
        sampleCount /= sampledFiles;

        //sample some records to get statistics information of input KV
        for (int i = 0; i < sampledFiles; i++) {
            FileStatus file = files.get(i);
            long start0 = System.currentTimeMillis();
            try {
                long count = parquetReader.readWithLimit(file, keyFieldName, (key, value) -> {
                    if (key.length > Common.MAX_KEY_SIZE | value.length > Common.MAX_VALUE_SIZE) {
                        logger.warn("record too large, dropped");
                        return;
                    }
                    rdbWriter.sample(key, value);
                }, sampleCount);

                if (!quietMode) {
                    logger.info("sampling {} rows of src file {} cost: {}", count, file, (System.currentTimeMillis() - start0));
                }
            } catch (Throwable e) {
                logger.error("error when sampling", e);
                throw new RuntimeException(e);
            }
        }
        rdbWriter.onSampleFinished();

        //build database KV file
        Common.runParallel(Integer.parseInt(System.getProperty("bsdb.build.threads", "8")), (pool, futures) -> {
            for (FileStatus file : files) {
                futures.add(pool.submit(() -> {
                    long start0 = System.currentTimeMillis();
                    try {
                        long count = parquetReader.read(file, keyFieldName, (key, value) -> {
                            readCount.getAndIncrement();
                            try {
                                if (key.length > Common.MAX_KEY_SIZE | value.length > Common.MAX_VALUE_SIZE) {
                                    logger.warn("record too large, dropped");
                                    return;
                                }
                                rdbWriter.put(key, value);
                                writeCount.getAndIncrement();
                            } catch (Throwable e) {
                                logger.error("error when insert records", e);
                                throw new RuntimeException("error when inserting record.", e);
                            }
                        });

                        if (!quietMode) {
                            logger.info("read {} rows from src file {} cost: {}", count, file, (System.currentTimeMillis() - start0));
                        }
                    } catch (Throwable e) {
                        logger.error("error when insert records", e);
                        throw new RuntimeException(e);
                    }
                }));
            }
        }, true);


        //build database index
        rdbWriter.build();
        if (!quietMode) {
            logger.info("build cost:{}", (System.currentTimeMillis() - start));
            logger.info("read: {}, write: {}", readCount.get(), writeCount.get());
        }

        if (verify) {
            //integrity verify
            if (!quietMode) {
                logger.info("Verifying data integrity...");
            }

            start = System.currentTimeMillis();
            SyncReader rdbReader = new SyncReader(outPath, false, false, false, false);
            AtomicLong count = new AtomicLong(0);

            Common.runParallel(Runtime.getRuntime().availableProcessors(), (pool, futures) -> {
                for (FileStatus file : files) {
                    futures.add(pool.submit(() -> {
                        try {
                            parquetReader.read(file, keyFieldName, (key, value) -> {
                                if (key.length > Common.MAX_KEY_SIZE | value.length > Common.MAX_VALUE_SIZE) {
                                    logger.warn("record too large, dropped");
                                    return;
                                }
                                byte[] readOut;
                                try {
                                    readOut = rdbReader.getAsBytes(key);
                                } catch (Exception e) {
                                    logger.error("error when query", e);
                                    throw new RuntimeException(e);
                                }
                                count.getAndIncrement();
                                if (!Common.bytesEquals(value, readOut)) {
                                    logger.error("read out value not match input, verify failed.");
                                    System.exit(2);
                                }
                            });
                        } catch (Throwable e) {
                            logger.error("error when query", e);
                            throw new RuntimeException(e);
                        }
                    }));

                }
            }, true);

            if (!quietMode) {
                logger.info("Integrity check done, cost: {}", (System.currentTimeMillis() - start));
            }
        }
    }

    private static CommandLine parseArgs(String[] args) {
        Options options = new Options();
        options.addOption("q", "quiet", false, "Quiet mode, disable all prints");
        options.addOption("i", "input", true, "Specify input file or directory");
        options.addOption("kf", "key-name", true, "Specify parquet field name which will use as for database key");
        options.addOption("c", "compact", false, "Use compact record layout on disk");
        options.addOption("z", "zstd", false, "Compression the db record file");
        options.addOption("o", "output", true, "Specify output directory, default to ./rdb");
        options.addOption("cb", "checksum-bits", true, "Specify checksum bit length, default to 4");
        options.addOption("v", "verify", false, "Verify integrity of generated index");
        options.addOption("a", "approximate", false, "Approximate mode, keys will not be stored, choosing proper checksum bits to meet false-positive query rate");
        options.addOption("ps", "pass-size", true, "Memory cache size in MB used when generation index, in approximate mode memory cost doubles");
        options.addOption("bs", "compress-block-size", true, "Block size for compression, default 1024");
        options.addOption("sc", "sample-count", true, "Sample record count, default 10000");
        options.addOption("ds", "dictionary-size", true, "Shared dictionary size in MB, default 1");
        options.addOption("temp", "temp-dir", true, "temp directory to store hash keys");
        options.addOption("nn", "name-node", true, "name node url, e.g. hdfs://localhost:9000");

        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            logger.error("Error parsing input args: ", e);
            System.exit(1);
            return null;
        }
    }
}
