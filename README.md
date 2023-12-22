#### bsdb
A build&amp;serve style readonly KV store.

[中文说明](README_CN.md)

#### What is a read-only database?

A read-only database only supports query operations online and does not support dynamic insertion or modification of records. The data in the database needs to be constructed through an offline build process before it can be deployed to serve online queries.

#### What is the usage scenario of a non-modifiable online database?

- One traditional scenario is the need for pre-built dictionary data that shipped with system/software releases. These data may only need to be updated when the system/software is upgraded.

- More recently scenarios are: big data systems need to serve data query APIs after data mining/processing, e.g. user profiling/feature engineering/ID mapping. The data supporting these query APIs usually are updated in batches on a scheduled basis, but each update involves replacing a large number of records, approaching a full replacement of the entire database/table. As an online data query service, it often needs to support a high query rate per second (QPS) while ensuring low and predictable query response times. When using traditional KV databases to support this scenario, one common problem encountered is the difficulty of guaranteeing the SLA of the query service during batch updates. Another common problem is that it usually requires configuring/maintaining expensive database clusters to support the SLA of the query service.

#### What are the advantages of a read-only database?

- cheap. During construction, disk IO can be fully optimized as large block sequential writes, which runs well with low-cost HDD or OSS; it is also very friendly to SSD writes, with near-zero write amplification, which is beneficial to optimize SSD lifetime, and using cheap consumer-grade SSD might be a reasonable choice in some case.
- Simple and reliable. No need to support for transactions/locks/logs/intra-instance communications, making the online serving logic very simple. The data is read-only during online, so there is no concern about data integrity being compromised.
- High performance. During the build phase, the full set of data can be accessed, allowing for the optimization of data organization based on the information available to facilitate queries.
- Low latency. The short read path and the lack of IO interference help to maintain high throughput and stable latency.
- Scalable. Multiple instances can be setup through simple file copy, achieving unlimited linear scaling with no performance penalty.

#### Matching Scenarios

- The number of records in the data set is very large (billions), or the data set is very large (several TB), do not expect to pay too much cost on server memory but high IOPS SSD costs can be affordable (SSD is still much cheaper than memory). 
- Large datasets with query performance requirements (can not support by HDD), expect the database compression ratio is higher to save the storage cost (SSD is still expensive relatively).
- updates of the data set does not must be seen by queried immediately, can serve queries via a batch updated historical snapshot version
- updates usually impact large number of records
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