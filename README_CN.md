BSDB是一种新型的只读KV数据库。

#### 什么是只读数据库？

 只读数据库在线只支持查询操作，不支持对记录进行动态插入和修改。数据库中的数据需要通过一个离线的过程来构建，构建完成后才可以上线提供在线查询服务。

#### 不能在线修改的数据库有什么用？

- 一种传统场景是需要随系统/软件发布的预置字典表，这些数据只有在系统/软件升级的时候才有可能需要更新。

- 更多场景则是大数据系统对数据加工后，通常需要对外提供数据查询服务；这些查询服务的支撑数据通常对更新的时效性要求不是特别高，很多时候是批量定时更新的，但每次更新都要替换大量的记录（几千万到几百亿），接近于整库/表全量替换。作为在线查询到数据服务，通常要支持很高qps的查询量，同时还得保证查询的响应时间比较低且确定，尽量不要有慢查询。利用传统KV数据库来支撑这种场景，最常碰到的一个问题就是定时对数据进行批量更新的时候，查询服务的SLA难以保证，通常需要降低更新操作的速率，这又导致更新耗时非常长；另一个常见问题则是通常需要配置/维护昂贵的数据库集群才能支持查询服务的SLA。

#### 只读数据库有什么优势？
- 简单可靠。构建时磁盘IO可以全部优化为大块顺序写，机械硬盘和对象存储也可以支撑; 对SSD也很友好，近乎0写入放大，有利于优化SSD寿命，甚至可以考虑利用廉价的消费级SSD。在线程序逻辑简单, 不需要支持事务/锁/日志/集群; 运行时数据只读，不用担心破坏数据完整性。
- 高性能。在构建的阶段可以看到全量数据，可以充分利用数据的信息来优化数据的组织形式以利于查询。在线查询阶段，不同的读查询互相不需要任何信息共享，易于并发处理。
- 低时延。读路径短，且IO没有干扰，有利于保持高的吞吐量和稳定的时延。
- 可扩展。通过简单复制来启动多个实例，达到真正无性能损失的无限线性扩展。

#### 适合的场景

- 数据集中记录数很多(几亿-几百亿)，或者数据集很大(几TB)，同时不期望为付出太多成本在服务器内存上，但可以承受高iops SSD的成本(SSD还是比内存便宜很多)
- 数据集很大，对查询性能也有要求(没法用机械盘支撑)，期望数据库压缩率比较高，节省存储成本(SSD也有点小贵)
- 数据集的更新不用很快就需要被查询到，可以接受读取批量更新的历史快照版本
- 数据集更新经常涉及大量记录，某些时候甚至类似整库替换
- 期望用尽量低的成本来达成很高的查询吞吐量(几万-百万qps)，很低的时延(次毫秒)或者很确定的时延


#### 特色功能
对于某些特定应用场景，应用对查询结果的准确性可以容忍一定的错误率，此时利用BSDB的模糊索引功能，能够大幅提高查询的qps(理论上相对普通索引模式增加一倍)。
- 对于包含在数据库中的key，可以保证查得正确结果
- 对于不包含在数据库中的key，查询可能返回null，也可能返回一条错误记录，其错误率和checksum bits相关
- 对应的value大小有限制，目前只会保存value的前8 bytes;且不管插入时是什么格式，查得的value会统一返回为byte[8]数组，应用层需要自行进行处理

#### 存储结构设计
BSDB是一种KV数据库，核心由配置文件，数据文件和索引文件构成。KV记录保存在数据文件里，索引文件则记录了某个Key对应的记录在数据文件内的位置，而配置文件中记录构建时的参数和数据库的统计信息。

##### 索引文件
BSDB的记录寻址由两级索引来完成。第一级索引为一个完美Hash函数，该函数会将输入的Key映射为一个整数，通过这个整数可以在第二级索引里对应的位置找到该Key对应的记录在数据文件内的地址。

完美Hash函数通过传入数据集的全量的Key来构建，构建完成后，可以生成一个函数，将N个Key映射为范围为1-N的整数，且不同的Key会映射到不同的数字。但当输入数据库中不存在的Key时，Hash函数可能会错误返回一个数字，此时就需要通过索引读取数据文件中保存的记录信息进行比对，才可以发现输入的Key在数据集中不存在;针对这个问题，可以预先保存Key的checksum，通过checksum比对可以提前发现这种情况，从而减少一部分磁盘的读需求。不同的checksum长度对应的false positive rate如下：

|checksum bits                          |false positive ratio |
|-------------------------------|-----------------------------|
|2            |12.5%            |
|4            |6.2%            |
|8            |0.39%           |
|10            |0.097%           |
|12            |0.025%           |

完美Hash函数运行时需要加载到内存，其大小为：记录数 x ((3+checksum) / 8) 字节。对于一个100亿的数据集，checksum选择5，则完美Hash函数需要接近10GB的HEAP内存。

第二级索引为一个基于磁盘的地址数组，每个地址预留的长度为8字节，因此第二级索引的大小为：记录数 x 8 字节。对于100亿记录的数据集，二级索引文件大概是80GB。二级索引文件运行时不需要加载到内存;如果系统的可用内存大于或者接近二级索引文件大小，则利用Buffered IO来读取索引文件，可以利用操作系统的页面缓存来加速;不过如果系统内存较小，则更适合利用Direct模式跳过系统缓存来读取二级索引文件，更有可能获得较高性能。

##### 数据文件
数据文件用于存储原始KV记录。
每个记录的格式如下：

    [record begin][record header][key][value][record end]  

记录的开头是record header，header的首字节记录Key的长度，第2-3字节记录Value的长度。record header后边是Key的具体内容，接下来是Value的具体内容。Key的长度支持1-255字节，而Value的长度目前限定为0-32510。

数据文件内记录的排布支持两种格式：紧凑和分块。紧凑模式格式如下：

    [file begin][record 1][record 2].....[record n][file end]

而分块模式下磁盘格式如下:

    [file begin][block 1][block 2].....[block n][file end]

记录会先按紧凑格式组装成Block，然后按Block写入数据文件; Block的大小为4K的倍数。单条记录不会跨Block存储，因此Block尾部可能会留下一部分未能利用的空间。对于记录大小大于一个Block的记录，会单独保存到一个Large Block， Large Block的大小也需要是4K的倍数，且Large Block尾部剩余的空间也不会用于保存其他记录。

紧凑模式节省磁盘空间，但查询时部分记录可能会跨越4K的边界，会对读性能有一些影响;分块模式下每个块都是对齐到4K的，对读更友好。另外分块模式下可以支持压缩。

目前数据库文件也支持压缩，压缩算法为ZSTD，压缩级别缺省为6(测试表明不一定压缩级别越高，压缩率就一定更好，也会有变差的情况)。压缩格式下数据文件的磁盘格式如下：

    [file begin][compressed block 1][compressed block 2].....[compressed block n][file end]

compressed block在磁盘上是紧凑排列; 后续计划增加compressed block按Page对齐的格式支持。每个compressed block内部的格式如下：

    [block begin][block header][block data][block end]

其中block header为8 bytes，1-2字节为压缩后数据的长度，3-4字节为压缩前数据的长度，5-8字节保留; 只是用2字节来表示长度是因为BSDB预期的场景更多是保存相对小的记录。block data部分则用于保存压缩后的block数据。



#### 构建过程

目前Builder工具的工作流程如下：

- 对输入文件进行采样，收集K，V的一些统计信息，压缩率相关的信息，并生成共享压缩字典
- 解析输入数据文件，构建BSDB数据文件，并保存所有的Key
- 利用保存的Key构建完美Hash
- 扫描BSDB数据文件，读取Key及KV记录的地址，查询完美Hash，构建索引文件

第三步构建索引文件时，索引内容在内存中构建，构建完成后批量写入磁盘，从而规避随机IO，让构建过程在基于机械硬盘或者对象存储的环境中也可以高效完成。在数据库记录数很多，索引不能完全放在内存中的情况下，Builder工具通过多次扫描数据文件，每次构建一部分索引来解决，此时通过ps参数来设定每次构建索引时需要的off-heap内存的大小。

#### 缓存
目前BSDB内部没有提供任何缓存，这主要基于几个考虑：
- 很多场景下查询输入的Key是完全随机的，且分布很散，缓存效率(命中/缓存大小)不高
- 如果数据集不是特别大，或者系统内存资源很丰富，OS的页面缓存也很有效
- 如果场景确有必要，应用层外挂一个缓存也是很成熟的方案
- 缓存维护增加复杂性，也有性能开销

当然对某些场景，查询还是有一定的局部性，此时通过内存还是能换来一定的性能提升，且现在云主机大都每CPU配置的内存比较高(4-8GB)，对于BSDB来说往往是CPU偏少而内存偏多，此时或许应该考虑引入Block Cache来充分利用内存。

#### 工具

##### 数据库构建工具
目前自带构建工具仅支持类似csv的文本输入，每行一个记录，记录格式为 <key><分隔符><value>。文件支持gz或者zstd压缩，压缩时文件名需要以.gz/.zstd结尾。其他格式的输入，可以参考Builder源码自行编写构建程序。

命令：java -cp bsdb-jar-with-dependencies.jar ai.bsdb.Builder -i <文本格式kv文件路径>

支持的参数：

- -i Specify input file or directory，指定输入的文本kv文件路径。
- -o Specify output directory, default to ./rdb， 指定数据库的输出目录，缺省为./rdb。
- -s Specify key/value separator, default to space " "，指定kv的分隔符，缺省为一个空格。
- -e Specify input file encoding, default to UTF-8，指定输入文本文件的字符编码，缺省为UTF-8。
- -c Use compact record layout on disk, 使用紧凑格式的数据文件; 缺省使用分块模式。
- -cb Specify checksum bit length, default to 4，指定构建的索引中的checksum的bit数，建议2-16.增大该选项对查询不在数据库中的记录有帮助，但是也会需要更多的内存(记录数乘bit数)
- -v Verify integrity of generated index, 命令行包含此参数，构建完成后，会查询/比对输入文件中的所有记录。
- -ps Memory cache size in MB used when generation index，指定生成索引时使用的缓存大小，单位为MB，缺省为1GB。注意打开生成模糊模式索引会同时构建两份索引(精确和模糊)，有双份的内存消耗。完整索引的大小为 记录数x8 bytes, 如果完整索引大小大于-ps指定的大小，则需要多次扫描数据库文件来生成索引。
- -z Compression the db record file,生成压缩的数据文件。
- -bs Block size for compression,压缩的块大小,缺省 4096 bytes。
- -a Approximate mode, keys will not be stored, choosing proper checksum bits to meet false-positive query rate,生成模糊查询模式索引。
- -temp Root directory for temp file, default to /tmp, should have enough space to store all keys, 构建完美Hash时临时文件的根目录，需要有足够的空间来保存所有的key，用于构建perfect hash函数，缺省是/tmp。


样例：

    java -ms4096m -mx4096m -verbose:gc   --illegal-access=permit   --add-exports java.base/jdk.internal.ref=ALL-UNNAMED  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED  -Djava.util.concurrent.ForkJoinPool.common.parallelism=16   -Dit.unimi.dsi.sux4j.mph.threads=16   -cp bsdb-jar-with-dependencies-0.1.2.jar   ai.bsdb.Builder -i ./kv.txt.zstd -ps 8192  

说明：
- -Djava.util.concurrent.ForkJoinPool.common.parallelism=16 用于控制写入数据的并发数量，建议设置为CPU的逻辑核心数量，以保证构建时可以充分利用CPU
- -Dit.unimi.dsi.sux4j.mph.threads=16 用于控制构建完美Hash的并发数量，也建议设置为CPU的逻辑核心数量


###### Parquet Builder
系统也提供了读取HDFS文件系统上打Parquet文件来构建数据库的工具：
命令： java -cp bsdb-jar-with-dependencies.jar:[hadoop jars] ai.bsdb.ParquetBuilder 

除了普通Builder的参数，ParquetBuilder还需要指定以下参数：
- -nn Name Node url，HDFS Name Node的地址
- -kf key field name，用于读取key的parquet column name

样例：


    java -ms8g -mx16g -XX:MaxDirectMemorySize=40g  --illegal-access=permit --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -Djava.util.concurrent.ForkJoinPool.common.parallelism=16 -Dit.unimi.dsi.sux4j.mph.threads=16 -cp ../bsdb-jar-with-dependencies-0.1.2.jar:/usr/local/apache/hadoop/latest/etc/hadoop:/usr/local/apache/hadoop/latest/share/hadoop/common/lib/*:/usr/local/apache/hadoop/latest/share/hadoop/common/*:/usr/local/apache/hadoop/latest/share/hadoop/hdfs:/usr/local/apache/hadoop/latest/share/hadoop/hdfs/lib/*:/usr/local/apache/hadoop/latest/share/hadoop/hdfs/*:/usr/local/apache/hadoop/latest/share/hadoop/mapreduce/*:/usr/local/apache/hadoop/latest/share/hadoop/yarn/lib/*:/usr/local/apache/hadoop/latest/share/hadoop/yarn/*: ai.bsdb.ParquetBuilder  -ps 30000 -z -bs 8192 -nn hdfs://xxxx:9800 -i  /xxx/data/all/2023/09/idfa_new_tags/ -ds 2 -sc 100000  -kf did_md5  -temp /data/tmp  


如果HDFS启用了Kerbros认证，启动程序前，需要确保当前登录系统已经通过Kerbros认证，有足够的权限访问HDFS。要是没有，需要运行kinit命令来进行认证，例如：

    kinit sd@HADOOP.COM -k -t ~/sd.keytab

根据部分内部数据集的测试结果，生成的压缩格式的BSDB数据库所有文件大小，大约是原始Parquet(gzip压缩)文件大小的70%，对存储成本的节省还是有比较好的效果; 当然这其中节省的部分应该有相当一部分是因为zstd相对gzip的压缩率优势，但这也表明行式数据库在使用合适的技巧和算法的情况下，不一定在压缩效率上就会比列式的差。


##### Web服务工具
系统提供了一个简易的基于Netty的Web查询服务。 命令： 
    
    java -cp bsdb-jar-with-dependencies.jar ai.bsdb.HttpServer -d <数据库文件的根目录>

支持的参数：
- -A Specify http listen port, default to 0.0.0.0
- -p Specify http listen port, default to 9999
- -d Specify data directory, default to ./rdb
- -P Specify http uri prefix, default to /bsdb/
- -t Specify worker thread number, default to processor count
- -a Approximate mode,模糊查询模式
- -ic Cache Index,index will be loaded to memory.Must have sufficient memory to load ./rdb/index.db，预加载全量索引到内存，需要确保系统有足够的内存(加载使Off-heap memory，因此不需要调整java的heap memory)
- -id Use direct IO to read index, 使用direct IO模式读取索引，在索引文件远大于内存时建议开启
- -kd Use direct IO to read KV file, 使用direct IO模式读取KV记录，在kv.db远大于内存时建议开启
- -async Use async mode to query db,使用异步模式访问数据库
- -json Deserialize stored values as json output, 将value转化为JSON格式输出，只适用于利用parquet文件构建的数据库
  
样例：

    java -ms4096m -mx4096m -verbose:gc --illegal-access=permit --add-exports java.base jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp bsdb-jar-with-dependencies-0.1.2.jar ai.bsdb.HttpServer -d ./rdb -kd -id    


缺省情况下可以通过 http://xxxx:9999/bsdb/<key>来查询数据。

##### API
基于性能考虑，目前BSDB提供作为内嵌数据库的查询API。

    import ai.bsdb.read.SyncReader;

    String dbPath = "./rdb";
    SyncReader db = new SyncReader(new File(dbPath), false, false, true, true);

    byte[] key = "key1".getBytes();
    byte[] value = db.getAsBytes(key);

同时也提供了对异步查询的支持：

    import ai.bsdb.read.AsyncReader;

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


##### 性能测试工具
系统提供了几个简单的查询性能测试工具，可以用于评估数据库的性能。

###### 同步查询性能测试工具
命令：

    java -cp bsdb-jar-with-dependencies.jar ai.bsdb.bench.QueryBench -d <数据库文件的根目录> -k <文本格式kv文件路径> [-s <separator>] [-a] [-ic] [-id] [-kd] [-v]

支持的参数：

- -d Specify data directory, default to ./rdb
- -k Specify input file for sequential query keys，用于查询的key文件，格式和用于build数据库的相同。
- -s Specify key/value separator, default to space " "，指定kv的分隔符，缺省为一个空格。
- -a Approximate mode,模糊查询模式
- -ic Cache Index,index will be loaded to memory.Must have sufficient memory to load ./rdb/index.db，加载索引到内存
- -id Use direct IO to read index, 使用direct IO模式读取索引，在索引文件远大于内存是建议开启
- -kd Use direct IO to read KV file, 使用direct IO模式读取KV记录，在kv.db远大于内存是建议开启
- -v Verify,检查查得的value和输入是否一致

同时需要通过-Djava.util.concurrent.ForkJoinPool.common.parallelism=<num> 来调整JVM的Common forkjoin pool的并发数量来控制数据库查询的并发; 同步模式下，通常需要比较高的并发才能充分发挥NVME SSD的性能。

样例：

    /home/hadoop/jdk-11.0.2/bin/java -ms16G -mx16G  -Djava.util.concurrent.ForkJoinPool.common.parallelism=320 --illegal-access=permit --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp bsdb-jar-with-dependencies-0.1.2.jar  ai.bsdb.bench.QueryBench -id -kd -v -k ../100e_id 

###### 异步查询性能测试工具
命令：

    java -cp bsdb-jar-with-dependencies.jar ai.bsdb.bench.AsyncQueryBench -d <数据库文件的根目录> -k <文本格式kv文件路径> [-s <separator>] [-a] [-id] [-kd] [-v]

支持的参数：

- -d Specify data directory, default to ./rdb
- -k Specify input file for sequential query keys，用于查询的key文件，格式和用于build数据库的相同。
- -s Specify key/value separator, default to space " "，指定kv的分隔符，缺省为一个空格。
- -a Approximate mode,模糊查询模式
- -id Use direct IO to read index, 使用direct IO模式读取索引，在索引文件远大于内存是建议开启
- -kd Use direct IO to read KV file, 使用direct IO模式读取KV记录，在kv.db远大于内存是建议开启
- -v Verify,检查查得的value和输入是否一致

同时支持如下的系统属性：

- bsdb.uring 使用IO Uring，缺省关闭。启用IO Uring，需要检查kernel版本，以及调高相关的limit， 比如:ulimit -n和limit -l
- bsdb.reader.index.submit.threads index文件并发读取线程数量
- bsdb.reader.kv.submit.threads 数据文件并发读取线程数量
- java.util.concurrent.ForkJoinPool.common.parallelism 系统Common forkjoin pool的并发线程数量，用于控制读取输入文件的并行

样例：

    /home/hadoop/jdk-11.0.2/bin/java -ms16G -mx16G -Dbsdb.uring=true -Djava.util.concurrent.ForkJoinPool.common.parallelism=3 -Dbsdb.reader.kv.submit.threads=10 -Dbsdb.reader.index.submit.threads=10 --illegal-access=permit --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp bsdb-jar-with-dependencies-0.1.2.jar    ai.bsdb.bench.AsyncQueryBench -id -kd -v -k ../100e_id 


###### 性能测试结果
测试环境：京东云 存储优化IO型实例 s.i3.4xlarge，配置 16 cpu cores，64GB内存，2 x 1862 NVMe SSD。

实测实例的CPU型号为：Intel(R) Xeon(R) Gold 6267C CPU @ 2.60GHz， 24核心，48超线程，应该是给实例分配了1/3的超线程核心。   

利用fio测试磁盘randread性能，io_uring和aio engine在bs=4K可以达到1300K iops， bs=8K大约700K iops，bs=16K大约380K iops。

测试数据集包含130亿条记录，详细情况如下：
    
    kv.compressed = false
    kv.compact = false
    index.approximate = false
    hash.checksum.bits = 4
    kv.count = 13193787549
    kv.key.len.max = 13
    kv.key.len.avg = 13
    kv.value.len.max = 36
    kv.value.len.avg = 32

BSDB同步查询性能：

|threads|16|32|48|64|80|96|112|128|144|160|176|192|208|224|240|256|272|286|302|318|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|qps|86212|154476|211490|258228|299986|338324|369659|399818|425796|448242|466551|487761|510411|529005|537797|544188|553712|554448|553728|558394|

同步查询需要较大的并发线程才可以充分利用磁盘IO，最高可以接近55万qps，接近但应该还没有达到SSD IO性能的极限(理论上每次查询2次IO，理想情况下应该能做到1300K iops/ 2 iops = 650K qps); 要是服务器的CPU配置再高一些，可能qps还有一定提升的空间。

同步查询启用模糊索引，可以达到100万qps; 异步查询模式下启用IO Uring，可以达到50万qps，相比同步模式qps没有绝对优势，不过更易于在CPU利用率和qps间平衡。 

#### 注意事项
- JDK版本支持9-11，暂不支持17等更高版本
- 操作系统目前支持x86_64 Linux，若使用IO Uring，需要kernel版本至少5.1x.
- 输入文件中的key不可以有重复,需要预先排重
- 构建工具可以基于传统磁盘运行，提供在线服务的时候需要SSD。由于每次查询需要两次磁盘IO，所以查询的qps大体等于磁盘随机IOPS/2，比如一个50万IOPS能力的SSD，理想情况下能达到接近20-25万查询QPS.模糊索引模式下只需要一次磁盘IO，理论上查询性能可以翻倍，不过只适用于部分对于查询结果可以容忍一定比例错误的场景（比如广告），并且Value的大小也有限制(目前是不超过8 Bytes)
- 紧凑模式下磁盘空间需求：记录数 x ((3+checksum) / 8 + 8 + 2) + key总大小×2 + value总大小
- 构建时内存需求：HEAP Memory： Perfect Hash常驻内存为：记录数 x ((3+checksum) / 8) Bytes + extra 2GB， 实测构建50亿记录大概需要6GB的heap memory。Off heap memory： cache size，由ps参数指定。
- 运行时内存需求：Perfect Hash常驻内存为：记录数 x ((3+checksum) / 8) Bytes + extra 2GB.例如对于记录数是2B，checksum是4bit，大概是1.7GB.

#### 后续计划

- 压缩相关的优化
- 更好的利用数据集的统计信息
- 基于C或Rust实现的Reader API
- Reader API的Python支持