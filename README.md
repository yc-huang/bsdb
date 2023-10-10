#### bsdb
A build&amp;serve style readonly kv store.

[中文说明](README_CN.md)

#### What is a read-only database?

A read-only database only supports query operations online and does not support dynamic insertion and modification of records. The data in the database needs to be constructed through an offline process before it can be deployed to serve online queries.

#### What is the usage scenario of a non-modifiable online database?

- One traditional scenario is the need for pre-defined dictionary data that shipped with system/software releases. These data may only need to be updated when the system/software is upgraded.

- More recently scenarios are: big data systems need to serve data query APIs after data mining/processing. The data supporting these query APIs usually are updated in batches on a scheduled basis, but each update involves replacing a large number of records, approaching a full replacement of the entire database/table. As an online data query service, it often needs to support a high query rate per second (QPS) while ensuring low and predictable query response times. When using traditional KV databases to support this scenario, one common problem encountered is the difficulty of guaranteeing the SLA of the query service during batch updates. Another common problem is that it usually requires configuring/maintaining expensive database clusters to support the SLA of the query service.

#### What are the advantages of a read-only database?

-    Simple and reliable. No need to support for transactions/locks/logs/clusters, making the online serving logic very simple. The data is read-only during online, so there is no concern about data integrity being compromised.
-    High performance. During the build phase, the full set of data can be accessed, allowing for the optimization of data organization based on the information available to facilitate queries.
-    Low latency. The read path is short, and I/O is not disturbed, which helps maintain stable latency.
-    Scalable. Multiple instances can be setup through file copy, achieving true painless linear scalability.

