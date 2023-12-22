package ai.bsdb.tools;

import ai.bsdb.read.SyncReader;
import ai.bsdb.util.Common;
import ai.bsdb.write.BSDBWriter;
import com.google.common.base.Strings;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class Builder {
    private static final String STORED_ENCODING = "UTF-8";
    private static final String DEFAULT_CHECKSUM_BITS = "4";
    private static final String DEFAULT_PASS_CACHE_SIZE = "1024";//1GB
    private static final String DEFAULT_SAMPLE_COUNT = "50000";
    private static final String DEFAULT_DICT_SIZE = "1";

    static Logger logger = LoggerFactory.getLogger(Builder.class);

    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);
        boolean quietMode = cmd.hasOption("q");
        boolean verify = cmd.hasOption("v");
        boolean compress = cmd.hasOption("z");
        boolean compact = cmd.hasOption("c");
        boolean approximateMode = cmd.hasOption("a");

        String inputFile = cmd.getOptionValue("i");
        if (Objects.isNull(inputFile)) {
            logger.error("must specify input file.");
            System.exit(1);
        }
        File inputPath = new File(inputFile);
        if (!inputPath.exists()) {
            logger.error("input file not exist:{}", inputPath);
            System.exit(1);
        }

        String out = cmd.getOptionValue("o", "./rdb/");
        File outPath = new File(out);
        if (outPath.exists()) {
            if (outPath.isFile()) {
                logger.error("output directory exist but not as directory:{}", out);
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
                    logger.error("temp directory exist but not as directory:{}", tempDir);
                    System.exit(1);
                }
            } else {
                tempDirFile.mkdirs();
            }
        }

        String separator = cmd.getOptionValue("s", " ");
        String encoding = cmd.getOptionValue("e", STORED_ENCODING);
        Charset charset = Charset.forName(encoding);
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

        File[] inputFiles = new File[]{inputPath};
        if (inputPath.isDirectory()) {
            inputFiles = inputPath.listFiles(
                    file -> !file.isDirectory() && (file.getName().endsWith(".txt") | file.getName().endsWith(".gz") | file.getName().endsWith(".zstd"))
            );
        }

        if (inputFiles == null || inputFiles.length == 0) {
            logger.error("no valid input file found.");
            System.exit(1);
        }
        ;

        int sampledFiles = Math.min(8, inputFiles.length); //no need to sample too much files
        sampleCount /= sampledFiles;
        //sample some records to get statistics information of input KV

        File[] finalInputFiles = inputFiles;
        int finalSampleCount = sampleCount;
        Common.runParallel(Common.CPUS, (pool, futures) -> {
            for (int i = 0; i < sampledFiles; i++) {
                File input = finalInputFiles[i];
                futures.add(pool.submit(() -> {
                    long start0 = System.currentTimeMillis();
                    try (BufferedReader reader = Common.getReader(input, charset)) {
                        reader.lines().limit(finalSampleCount).map(line -> line.split(separator)).forEach(cols -> {
                            if (cols.length == 2) {
                                try {
                                    byte[] key = cols[0].getBytes(STORED_ENCODING);
                                    byte[] value = cols[1].getBytes(STORED_ENCODING);
                                    if (key.length > Common.MAX_KEY_SIZE | value.length > Common.MAX_VALUE_SIZE) {
                                        logger.error("record too large, dropped: {}", Arrays.toString(cols));
                                        return;
                                    }
                                    rdbWriter.sample(key, value);
                                } catch (IOException e) {
                                    logger.error("error when sampling", e);
                                    //throw new RuntimeException("error when sampling record.", e);
                                }
                            }
                        });

                        if (!quietMode) {
                            logger.info("sampling src file {} cost: {}", input, (System.currentTimeMillis() - start0));
                        }
                    } catch (Exception e) {
                        logger.error("error when sampling", e);
                    }
                }));

            }
        }, true);
        rdbWriter.onSampleFinished();

        //build database KV file

        Common.runParallel(Common.CPUS, (pool, futures) -> {
            for (File input : finalInputFiles) {
                futures.add(pool.submit(() -> {
                    long start0 = System.currentTimeMillis();
                    try (BufferedReader reader = Common.getReader(input, charset)) {
                        reader.lines().map(line -> line.split(separator)).forEach(cols -> {
                            readCount.getAndIncrement();
                            if (cols.length == 2) {
                                try {
                                    byte[] key = cols[0].getBytes(STORED_ENCODING);
                                    byte[] value = cols[1].getBytes(STORED_ENCODING);
                                    if (key.length > Common.MAX_KEY_SIZE | value.length > Common.MAX_VALUE_SIZE) {
                                        logger.warn("record too large, dropped: {}", Arrays.toString(cols));
                                        return;
                                    }
                                    rdbWriter.put(key, value);
                                    writeCount.getAndIncrement();
                                } catch (IOException | InterruptedException e) {
                                    logger.error("error when inserting record.", e);
                                    //throw new RuntimeException("error when inserting record.", e);
                                }
                            }
                        });
                    } catch (Exception e) {
                        logger.error("error when inserting", e);
                    }

                    if (!quietMode) {
                        logger.info("read src file {} cost: {}}", input, (System.currentTimeMillis() - start0));
                    }
                }));
            }
        }, true);
        //build database index
        rdbWriter.build();
        if (!quietMode) {
            logger.info("build cost: {}", (System.currentTimeMillis() - start));
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
            Common.runParallel(Common.CPUS * 2, (pool, futures) -> {
                for (File input : finalInputFiles) {
                    futures.add(pool.submit(() -> {
                        try (BufferedReader reader = Common.getReader(input, charset)) {
                            reader.lines().map(line -> line.split(separator)).forEach(cols -> {
                                if (cols.length == 2) {
                                    try {
                                        byte[] key = cols[0].getBytes(STORED_ENCODING);
                                        byte[] value = cols[1].getBytes(STORED_ENCODING);
                                        if (key.length > Common.MAX_KEY_SIZE | value.length > Common.MAX_VALUE_SIZE) {
                                            logger.warn("record too large, dropped: {}", Arrays.toString(cols));
                                            return;
                                        }
                                        byte[] readOut = rdbReader.getAsBytes(key);
                                        count.getAndIncrement();
                                        if (!Common.bytesEquals(value, readOut)) {
                                            logger.error("read out value not match input, verify failed.");
                                            System.exit(2);
                                        }
                                    } catch (Exception e) {
                                        logger.error("error when querying record.", e);
                                        throw new RuntimeException("error when querying record.", e);
                                    }
                                }
                            });
                        } catch (Exception e) {
                            logger.error("error when querying record.", e);
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
        options.addOption("c", "compact", false, "Use compact record layout on disk");
        options.addOption("z", "zstd", false, "Compression the db record file");
        options.addOption("o", "output", true, "Specify output directory, default to ./rdb");
        options.addOption("s", "separator", true, "Specify key/value separator, default to space\" \"");
        options.addOption("e", "encoding", true, "Specify input file encoding, default to UTF-8");
        options.addOption("cb", "checksum-bits", true, "Specify checksum bit length, default to 4");
        options.addOption("v", "verify", false, "Verify integrity of generated index");
        options.addOption("a", "approximate", false, "Approximate mode, keys will not be stored, choosing proper checksum bits to meet false-positive query rate");
        options.addOption("ps", "pass-size", true, "Memory cache size in MB used when generation index, in approximate mode memory cost doubles");
        options.addOption("bs", "compress-block-size", true, "Block size for compression, default 1024");
        options.addOption("sc", "sample-count", true, "Sample record count, default 50000");
        options.addOption("ds", "dictionary-size", true, "Shared dictionary size in MB, default 1");
        options.addOption("temp", "temp-dir", true, "temp directory to store hash keys");

        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            logger.error("Error parsing input args", e);
            System.exit(1);
            return null;
        }
    }
}
