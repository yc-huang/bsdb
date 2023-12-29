#### bsdb
A build&amp;serve style readonly KV store.

[中文说明](README_CN.md)

#### What is a read-only database?

A read-only database only supports query operations online and does not support dynamic insertion or modification of records. The data in the database needs to be constructed through an offline build process before it can be deployed to serve online queries.

#### What is the usage scenario of a non-modifiable online database?

- One traditional scenario is the need for pre-built dictionary data that shipped with system/software releases. These data may only need to be updated when the system/software is upgraded.

- More recently scenarios are: big data systems need to serve data query APIs after data mining/processing, e.g. user profiling/feature engineering/embeddings/ID mapping. The data supporting these query APIs usually are updated in batches on a scheduled basis, but each update involves replacing a large number of records, approaching a full replacement of the entire database/table. As an online data query service, it often needs to support a high query rate per second (QPS) while ensuring low and predictable query response times. When using traditional KV databases to support this scenario, one common problem encountered is the difficulty of guaranteeing the SLA of the query service during batch updates. Another common problem is that it usually requires configuring/maintaining expensive database clusters to support the SLA of the query service.

#### What are the advantages of a read-only database?

- Cheap. During construction, disk IO can be fully optimized as large block sequential writes, which runs well with low-cost HDD or OSS; it is also very friendly to SSD writes, with near-zero write amplification, which is beneficial to optimize SSD lifetime, and using cheap consumer-grade SSD might be a reasonable choice in some case.
- Simple and reliable. No need to support for transactions/locks/logs/intra-instance communications, making the online serving logic very simple. The data is read-only during online, so there is no concern about data integrity being compromised.
- High performance. During the build phase, the full set of data can be accessed, allowing for the optimization of data organization based on the information available to facilitate queries.
- Low latency. The short read path and the lack of IO interference help to maintain high throughput and stable latency.
- Scalable. Multiple instances can be setup through simple file copy, achieving unlimited linear scaling with no performance penalty.

#### Matching Scenarios

- The number of records in the data set is very large (billions), or the data set is very large (several TB), do not expect to pay too much cost on server memory but high IOPS SSD costs can be affordable (SSD is still much cheaper than memory). 
- Large datasets with high query performance(throughput or latency) required (can not serve on HDD), expect the database compression ratio to be higher to save the storage cost (SSD is still expensive relatively).
- Updates of the dataset does not must be seen by queries immediately, so queries could be served via a batch updated historical snapshot version
- Updates usually impact large amount of records, which is painful for most well-known databases
- Expect to achieve very high query throughput (tens of thousand to millions of qps) with minimal latency (sub-millisecond) or very predictable latency at the lowest possible cost.

#### Unique Features

For some specific application scenarios, the application can tolerate some inaccurate query results. In this case, by using the approximate indexing feature of BSDB, the qps of queries can be significantly increased (theoretically increasing by onefold compared to the normal indexing mode).

- For keys in the database, the query will always return correct result.
- For keys not in the database, the query may return null or a wrong random record. The error rate is related to the checksum bits setting.
- The size of the corresponding value is limited by now: only the first 8 bytes of the value will be kept. Regardless of the value when inserting, the value obtained by querying will be uniformly returned as a byte[8] array. The application layer needs to process it by itself.

#### On Disk Storage Structure

BSDB is a KV database that consists of a configuration file, a series data files, and an index file. KV records are stored in data files, while the index file stores the location of a certain key in data files. The configuration file stores the parameters used t built and some statistical information of the dataset.

##### Index file

BSDB uses a two-level indexing mechanism to locate records. The first level index is a minimal perfect hash function. This hash function maps the input key to an unique integer. Through this integer, the actual location of the record in data files can be found in the second level index.

The perfect hash function is constructed by collecting the full set of keys in the data set. After construction is completed, a function can be generated to map N keys to integers in the range of 1-N, and different keys will be mapped to different numbers. When querying a key that does not exist in the database, the hash function may incorrectly return a number. At this time, the stored record needs to be read to compare and determine that the input key does exist in the data set or not. To address this issue, a checksum of the key can be stored in advance. By comparing the checksum, this situation could be discovered earlier, thereby reducing lots of disk reads. The false positive rate of different checksum lengths is as follows:

|checksum bits	|false positive ratio|
|--|---|
|2	|12.5%|
|4	|6.2%|
|8	|0.39%|
|10|0.097%|
|12|0.025%|

The perfect hash function needs to be loaded into memory during serving. Its size can be calculated by: the number of records x ((3+checksum)/8) bytes. For a data set of 10 billion records, if the checksum is chosen to be 5, the perfect hash function might consume approximately 10GB of heap memory.

The second level index is a disk-based address array. Each address reserves a length of 8 bytes, so the size of the second level index is: number of records x 8 bytes. For a data set of 10 billion records, the second level index file is approximately 80GB. The second level index file does not must be loaded into memory during runtime. If the available memory is larger enough or close to the size of the second level index file, the Buffered IO should be choosed to read the index file to take advantage of the page cache of the operating system. However, if the system memory is small by compare to index file size, it is more suitable to use the Direct IO mode to bypass the system cache to read the second level index file which might obtain higher performance.

##### Data Files

The data files are used to store the original KV records. Records are in the following format:

    [record begin][record header][key][value][record end]

The first byte of record header stores the length of the key, and the second and third bytes of record header stores the length of the value. Then follows the raw content of the key, and then the raw content of the value. The length of the key supports 1-255, and the length of the value is currently limited to 0-32510.

The arrangement of records in the data file supports two formats: compact and blocked. The compact mode format is as follows:

    [file begin][record 1][record 2].....[record n][file end]

The blocked format is as follows:

    [file begin][block 1][block 2].....[block n][file end]

In blocked mode, records are first assembled into blocks using compact format and then written to the data file in blocks. The size of the block should be multiple of 4096. A single record will not be stored across blocks. Therefore, the end of the block may leave a portion of unused space. 
For records larger than one block, they will be stored separately in a Large Block. The size of the Large Block also needs to be a multiple of 4K, and the remaining space at the end of the Large Block will not be used to store other records.

The compact mode use less disk space but may affect read performance since some records read might cross the 4K boundary during query. The block mode makes each block aligned to 4K, making it more friendly to IO for read.

The database file currently supports compression as well, and the chosen compression algorithm is ZSTD. The compression level is default to 6 (tests show that higher compression levels do not necessarily result in better compression rates and may have variations). The disk format of the compressed data file is as following:

    [file begin][compressed block 1][compressed block 2]

Compressed blocks are arranged compactly on disk. There are plans to add support for aligned compressed blocks in the future. The internal format of each compressed block is as follows:

    [block begin][block header][block data][block end]

The block header is 8 bytes long, where the first two bytes represent the length of the compressed data, the next two bytes represent the length of the original uncompressed data, and the remaining 4 bytes are reserved. Using only 2 bytes to represent the length is because BSDB expects to handle relatively small records. The block data section is used to store the block data after compressing.

#### Build Process

The current workflow of the Builder tool is as follows:

- Sample the input files to collect some statistical information about K and V, including compression-related details, and generate a prebuilt shared compression dictionary.
- Parse the input data files to construct BSDB data files and collect all the keys. 
- Build a perfect hash using the collected keys.
- Scan the BSDB data files, iterate through all keys and collect the addresses of corresponding KV records; query the perfect hash, and construct the index file.

During the third step of building the index file, the index content is constructed in memory and then written to disk in batch to avoid random I/O. This allows the build process to be efficiently completed in environments based on HDD or cloud object storage. In cases where the database has a large number of records and the entire index cannot fit in memory, the Builder tool solves this by scanning the data files multiple times and building a portion of the index each time. The size of the off-heap memory used to hold index can be set using the "ps" parameter.

#### Caching

Currently, BSDB does not provide any internal caching, and this is based on several considerations:

- In many scenarios, the input keys for queries are completely random and widely distributed, resulting in very low cache efficiency (cache hits/cache size).
- If system memory size is on par with active dataset, the OS page cache should work well.
- If necessary, plugin an external cache at the application layer is a mature solution.
- Maintaining caches adds complexity and incurs performance overhead.

However, for certain scenarios, there is still some locality in queries, and in such cases, using memory more aggressively can achieve performance improvements. Moreover, most cloud instances nowadays have a relatively high amount of memory per CPU configuration (4-8GB), which often means that the server running BSDB tends to have fewer CPUs and more memory as expected. In such cases, it might be worth considering to introduce a Block Cache to fully utilize the available memory.

#### Tools
##### Builder

The built-in Builder tool currently supports text input formats similar to CSV, with one record per line in the format of <key><delimiter><value>. The input file can be compressed with gzip or zstd, and the corresponding compressed file name should have the extension of .gz or .zstd. For other input formats, you can refer to the Builder source code to write your own construction program.

Command: 

    java -cp bsdb-jar-with-dependencies.jar tech.bsdb.Builder -i <text_format_kv_file_path>

Supported parameters:

-    -i: Specify input file or directory.
-    -o: Specify output directory, default to ./rdb.
-    -s: Specify key/value separator, default to space " ".
-    -e: Specify input file encoding, default to UTF-8.
-    -c: Use compact record layout on disk, default: False, use block mode.
-    -cb: Specify checksum bit length, default to 4. It is recommended to set it between 2 and 16. Increasing this helps to improve performance with queries for records not in the database, but it also requires more heap memory (number of records * bit length).
-    -v: Verify integrity of the generated index. If this parameter is included in the command line, it will query and compare all records in the input file after construction.
-    -ps: Memory cache size in MB used when generating the index. The default is 1 GB. Note that enabling the generation of approximate index (-a) will build two copies of the index (exact and approximate), resulting in double memory consumption. The size of the full index is calculated as the number of records multiplied by 8 bytes. If the size of the full index exceeds the value specified by -ps, the database files need to be scanned multiple times to generate the index.
-    -z: Compression the db record files.
-    -bs: Block size in bytes for compression, the default is 4096 bytes.
-    -a: generate approximate mode index, choosing proper checksum bits to meet the expected false-positive query rate.
-    -temp: Root directory for temp files, default to /tmp. It should have enough space to store all keys when building a perfect hash.

Example:

    java -ms4096m -mx4096m -verbose:gc --illegal-access=permit --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -Djava.util.concurrent.ForkJoinPool.common.parallelism=16 -Dit.unimi.dsi.sux4j.mph.threads=16 -cp bsdb-jar-with-dependencies-0.1.2.jar tech.bsdb.tools.Builder -i ./kv.txt.zstd -ps 8192

Note:

-    -Djava.util.concurrent.ForkJoinPool.common.parallelism=16 is used to control the number of concurrent inserts. It is recommended to set it to the number of logical cores of the CPU to fully utilize the CPU during construction.
-    -Dit.unimi.dsi.sux4j.mph.threads=16 is used to control the number of concurrent operations when building the perfect hash. It is also recommended to set it to the number of logical cores of the CPU.

##### Parquet Builder

BSDB also provides a tool to build a database by reading Parquet files on the HDFS file system:

Command: 

    java -cp bsdb-jar-with-dependencies.jar:[hadoop jars] tech.bsdb.ParquetBuilder

In addition to the parameters of the regular Builder, ParquetBuilder requires the following extra parameters:

    -nn: Name Node URL, the address of the HDFS Name Node.
    -kf: Key field name, the Parquet column name used to read the key.

Example:

    java -ms8g -mx16g -XX:MaxDirectMemorySize=40g  --illegal-access=permit --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -Djava.util.concurrent.ForkJoinPool.common.parallelism=16 -Dit.unimi.dsi.sux4j.mph.threads=16 -cp ../bsdb-jar-with-dependencies-0.1.2.jar:/usr/local/apache/hadoop/latest/etc/hadoop:/usr/local/apache/hadoop/latest/share/hadoop/common/lib/*:/usr/local/apache/hadoop/latest/share/hadoop/common/*:/usr/local/apache/hadoop/latest/share/hadoop/hdfs:/usr/local/apache/hadoop/latest/share/hadoop/hdfs/lib/*:/usr/local/apache/hadoop/latest/share/hadoop/hdfs/*:/usr/local/apache/hadoop/latest/share/hadoop/mapreduce/*:/usr/local/apache/hadoop/latest/share/hadoop/yarn/lib/*:/usr/local/apache/hadoop/latest/share/hadoop/yarn/*: tech.bsdb.tools.ParquetBuilder  -ps 30000 -z -bs 8192 -nn hdfs://xxxx:9800 -i  /xxx/data/all/2023/09/idfa_new_tags/ -ds 2 -sc 100000  -kf did_md5  -temp /data/tmp  

If HDFS is enabled with Kerberos authentication, before starting the program, make sure that the current system login has been authenticated with Kerberos and has sufficient permissions to access HDFS. If not, you need to run the 'kinit' command for authentication, for example:

    kinit sd@HADOOP.COM -k -t ~/sd.keytab

Based on the test results of some internal datasets, the size of the generated compressed BSDB database files is about 70% of the size of the original Parquet files (compressed with gzip). Of course, a significant portion of the savings is due to the superior compression ratio of zstd compared to gzip. However, this also indicates that row-based databases, when using appropriate techniques and algorithms, may not necessarily have lower compression efficiency compared to columnar databases.

##### Web Service Tool

The system provides a simple HTTP query service based on Netty. Command:

    java -cp bsdb-jar-with-dependencies.jar tech.bsdb.HttpServer -d <root directory of the database file>

Supported parameters:

-    -A Specify the HTTP listen port, default to 0.0.0.0
-    -p Specify the HTTP listen port, default to 9999
-    -d Specify the data directory, default to ./rdb
-    -P Specify the HTTP URI prefix, default to /bsdb/
-    -t Specify the number of worker threads, default to the cpu count
-    -a Approximate mode
-    -ic Cache Index, the index will be fully loaded into memory. Preloading the full index(./rdb/index.db) into memory requires enough system memory (loaded as Off-heap memory, so no need to adjust Java's heap memory)
-    -id Use direct IO to read index file, recommended when the index file is much larger than memory
-    -kd Use direct IO to read KV files, recommended when kv.db is much larger than memory
-    -async Use async mode to query the database
-    -json Deserialize stored values as JSON output, only applicable to databases built using Parquet Builder

Example:

    java -ms4096m -mx4096m -verbose:gc --illegal-access=permit --add-exports java.base jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp bsdb-jar-with-dependencies-0.1.2.jar tech.bsdb.tools.HttpServer -d ./rdb -kd -id

By default, you can query data using http://xxxx:9999/bsdb/<key>.

##### API

Maven dependency：

    <dependency>
      <groupId>tech.bsdb</groupId>
      <artifactId>bsdb-core</artifactId>
      <version>0.1.2</version>
    </dependency>

For performance considerations, BSDB currently provides query APIs as an embedded database:

    import tech.bsdb.read.SyncReader;
    
    String dbPath = "./rdb";
    SyncReader db = new SyncReader(new File(dbPath), false, false, true, true);
    
    byte[] key = "key1".getBytes();
    byte[] value = db.getAsBytes(key);

Async query API is also supported:

    import tech.bsdb.read.AsyncReader;
    
    String dbPath = "./rdb";
    AsyncReader db = new AsyncReader(new File(dbPath), false, true, true);
    
    byte[] key = "key1".getBytes();
    db.asyncGet(key, null, new CompletionHandler<byte[], Object>() {
        @Override
        public void completed(byte[] value, Object object) {
        logger.debug("query return {} for key:{}", Arrays.toString(value), Arrays.toString(key));
        }
    
        @Override
        public void failed(Throwable throwable, Object objects) {
            logger.error("query failed", throwable);
        }
    });


##### Performance Testing Tools

The system provides several simple query performance testing tools that can be used to evaluate the performance of the database.
###### Synchronous Query Performance Testing Tool

Command:

    java -cp bsdb-jar-with-dependencies.jar tech.bsdb.bench.QueryBench -d <database root directory> -k <text format kv file path> [-s <separator>] [-a] [-ic] [-id] [-kd] [-v]

Supported parameters:

-    -d Specify data directory, default to ./rdb
-    -k Specify input file for loading query keys, the format is the same as the one used for building the database.
-    -s Specify key/value separator, default to space " ".
-    -a Approximate mode for queries.
-    -ic Cache Index, the index will be loaded into memory. Sufficient memory is required to load ./rdb/index.db.
-    -id Use direct IO to read the index. Recommended when the index file is much larger than memory.
-    -kd Use direct IO to read the KV file. Recommended when kv.db is much larger than memory.
-    -v Verify the consistency of the retrieved value with the input.

In addition, it is necessary to adjust the concurrency of the JVM's Common ForkJoin Pool for database queries by using the flag -Djava.util.concurrent.ForkJoinPool.common.parallelism=<num>. In synchronous mode, a relatively high concurrency is usually required to fully utilize the performance of NVME SSD.

Example:

    /home/hadoop/jdk-11.0.2/bin/java -ms16G -mx16G -Djava.util.concurrent.ForkJoinPool.common.parallelism=320 --illegal-access=permit --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp bsdb-jar-with-dependencies-0.1.2.jar  tech.bsdb.bench.QueryBench -id -kd -v -k ../100e_id

###### Asynchronous Query Performance Testing Tool

Command:

    java -cp bsdb-jar-with-dependencies.jar tech.bsdb.bench.AsyncQueryBench -d <database root directory> -k <text format kv file path> [-s <separator>] [-a] [-id] [-kd] [-v]

Supported parameters:

-    -d Specify data directory, default to ./rdb
-    -k Specify input file for sequential query keys, the format is the same as the one used for building the database.
-    -s Specify key/value separator, default to space " ".
-    -a Approximate mode for queries.
-    -id Use direct IO to read the index. Recommended when the index file is much larger than memory.
-    -kd Use direct IO to read the KV file. Recommended when kv.db is much larger than memory.
-    -v Verify the consistency of the retrieved value with the input.

It also supports the following system properties:

-    bsdb.uring: Enable IO Uring. Disabled by default. Enabling IO Uring requires checking the kernel version and adjusting related limits, such as ulimit -n and limit -l.
-    bsdb.reader.index.submit.threads: Number of concurrent threads for reading index files.
-    bsdb.reader.kv.submit.threads: Number of concurrent threads for reading data files.
-    java.util.concurrent.ForkJoinPool.common.parallelism: Number of concurrent threads in the system's Common ForkJoin Pool, used to control parallel reading of input files.

Example:

    /home/hadoop/jdk-11.0.2/bin/java -ms16G -mx16G -Dbsdb.uring=true -Djava.util.concurrent.ForkJoinPool.common.parallelism=3 -Dbsdb.reader.kv.submit.threads=10 -Dbsdb.reader.index.submit.threads=10 --illegal-access=permit --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp bsdb-jar-with-dependencies-0.1.2.jar tech.bsdb.bench.AsyncQueryBench -id -kd -v -k ../100e_id

###### Performance Test Results

Test Environment: 

    JD Cloud, Storage-Optimized IO Instance s.i3.4xlarge, with 16 CPU cores, 64GB memory, and 2 x 1862 NVMe SSDs.

The allocated instance is running on this CPU model: Intel(R) Xeon(R) Gold 6267C CPU @ 2.60GHz, 24 cores, 48 threads, with approximately 1/3 of the cores allocated to the instance.

Using fio to test the disk randread performance, the io_uring and aio engines can achieve around 1300K IOPS with bs=4K, approximately 700K IOPS with bs=8K, and around 380K IOPS with bs=16K.

The test dataset contains 13 billion records, with the following statistics:

    kv.compressed = false
    kv.compact = false
    index.approximate = false
    hash.checksum.bits = 4
    kv.count = 13193787549
    kv.key.len.max = 13
    kv.key.len.avg = 13
    kv.value.len.max = 36
    kv.value.len.avg = 32

BSDB synchronous query performance:

|threads|16|32|48|64|80|96|112|128|144|160|176|192|208|224|240|256|272|286|302|318|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|qps|86212|154476|211490|258228|299986|338324|369659|399818|425796|448242|466551|487761|510411|529005|537797|544188|553712|554448|553728|558394|

Synchronous queries require a higher level of concurrent threads to fully utilize disk IO. The maximum achievable QPS is close to 550,000, which is approaching but still below the limit of SSD IO performance (theoretical maximum of 1,300K IOPS divided by 2 IOPS per query, ideally reaching 650,000 QPS). If the server's CPU configuration were higher, there might be room for further QPS improvement.

Enabling approximate mode in synchronous queries can achieve about 1 million QPS. In asynchronous query mode with IO Uring enabled, it can reach 500,000 QPS. Compared to synchronous mode, there is no absolute advantage in QPS, but it provides a better balance between CPU utilization and QPS.

#### Notes

-    JDK version 9-11 is supported, but higher versions like 17 are not currently supported.
-    The operating system currently supports x86_64 Linux. If using IO Uring, the kernel version needs to be at least 5.1x, and you need to install liburing.so to your system (check https://github.com/axboe/liburing). 
-    The input file's keys must not have duplicates and need to be deduplicated in advance.
-    The Builder tool can run on traditional disks, but SSDs are required for online serving to achieve reasonable performance unless your datasets are small. Since each query requires two disk IOs, the QPS of the query is roughly equal to the disk's random read IOPS divided by 2. For example, with an SSD capable of 500,000 IOPS, in ideal conditions, it can achieve approximately 200,000 to 250,000 query QPS. In approximate indexing mode, only one disk IO operation is required, theoretically doubling the query performance. However, it is only suitable for limited scenarios (e.g., advertising), and there are also limitations on the size of the value (currently not exceeding 8 bytes, might be tunable in future).
-    In compact mode, the disk space requirement is: record count x ((3 + checksum) / 8 + 8 + 2) + total key size + total value size.
-    Heap memory requirement during build time: Perfect Hash resident memory is approximately: record count x ((3 + checksum) / 8) Bytes, plus some extra 2GB. In actual testing, building 5 billion records requires around 6GB of heap memory. Off-heap memory: cache size, specified by the ps parameter.
-    Heap memory requirement during runtime: Perfect Hash resident memory is approximately: record count x ((3 + checksum) / 8) Bytes + extra 2GB. For example, with 2 billion records and a 4-bit checksum, it would be approximately 1.7GB.

#### Future Plans

-    Compression related optimization
-    Better utilization of statistical information
-    Reader API implemented in C or Rust
-    Python support for the Reader API




