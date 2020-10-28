# balboa-backend-accumulo

![Java CI](https://github.com/satta/balboa-backend-accumulo/workflows/Java%20CI/badge.svg)

This is a backend for [balboa](https://github.com/DCSO/balboa) that uses
[Apache Accumulo](https://accumulo.apache.org/) as a storage and query engine.
It is quite basic in its feature set and should be considered a starting point or
building block in a more refined setup, most likely involving multiple input
consumer frontends feeding into multiple backend instances, all connecting to one
Accumulo cluster.

## Requirements

 * JDK 8 or later
 * [balboa-backend-java](https://github.com/satta/balboa-backend-java) ([Maven Central](https://search.maven.org/artifact/com.github.satta/balboa-backend-java))
 * accumulo-core API 2.0
 * commons-cli

## Building

A self-contained jar can be built, in the source directory, like this:

```
$ mvn package
```

This should leave a `balboa-backend-accumulo-<VERSION>-jar-with-dependencies.jar`
in the `target/` subdirectory. Dependencies will be fetched automatically from
Maven Central.

## Configuration

The jar takes a `-c` command line parameter specifying the path to a
properties file, which needs to contain at least the necessary [Accumulo client
properties](https://accumulo.apache.org/docs/2.x/configuration/client-properties)
needed to connect to the cluster. For example, a simple development setup using
[Uno](https://github.com/apache/fluo-uno) could be accessed with something along the lines of:

```
instance.name=uno
instance.zookeepers=uno
auth.type=password
auth.principal=satta
auth.token=satta

balboa.port=4242
```

The `balboa.port` property defines the local port listened on for msgpack TCP connection
from frontends.

## Accumulo setup

The observation data are stored in three tables, optimized for `rrname`, `rdata` and
reverse `rrname` lookups (used for suffix queries). We store observations redundantly
reduce the number of indirections.

### Table `balboa_by_rrname`

| Row ID                       | Column Family | Column Qualifier | Visibility | Value       |
|------------------------------|---------------|------------------|------------|-------------|
| rrname-rsensorid-data-rrtype | count         | count            | public     | LONG VARLEN |
| rrname-rsensorid-data-rrtype | seen          | first            | public     | LONG VARLEN |
| rrname-rsensorid-data-rrtype | seen          | last             | public     | LONG VARLEN |

We use various combiners to aggregate identical observations:

```
setiter -class org.apache.accumulo.core.iterators.user.MaxCombiner -p 11 -t balboa_by_rrname -all      # on seen:last
setiter -class org.apache.accumulo.core.iterators.user.MinCombiner -p 13 -t balboa_by_rrname -all      # on seen:first
setiter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 12 -t balboa_by_rrname -all  # on count:count
```

These need to be set on the following  other tables as well:

### Table `balboa_by_rdata`

| Row ID                       | Column Family | Column Qualifier | Visibility | Value       |
|------------------------------|---------------|------------------|------------|-------------|
| rdata-sensorid-rrname-rrtype | count         | count            | public     | LONG VARLEN |
| rdata-sensorid-rrname-rrtype | seen          | first            | public     | LONG VARLEN |
| rdata-sensorid-rrname-rrtype | seen          | last             | public     | LONG VARLEN |

### Table `balboa_by_rrname_rev`

| Row ID                            | Column Family | Column Qualifier | Visibility | Value       |
|-----------------------------------|---------------|------------------|------------|-------------|
| rev(rrname)-sensorid-rdata-rrtype | count         | count            | public     | LONG VARLEN |
| rev(rrname)-sensorid-rdata-rrtype | seen          | first            | public     | LONG VARLEN |
| rev(rrname)-sensorid-rdata-rrtype | seen          | last             | public     | LONG VARLEN |

## PoC limitations

 * Hard-coded table names and `public` visibility
 * Wildcard support limited to `rrname` queries
 * For `rrname` queries, additional `rdata` and `sensorid` constraints will be matched anywhere in the row