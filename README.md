<div align="center">
    <h1>wd2graph</h1>
    <p>
    A program for efficiently extracting the graph structure from a Wikidata truthy N-Triples dump.
    </p>
</div>
<p align="center">
    <a href="https://crates.io/crates/wd2graph">
        <img alt="Release" src="https://img.shields.io/crates/v/wd2graph">
    </a>
    <a href="https://docs.rs/wd2graph">
        <img alt="Docs" src="https://img.shields.io/docsrs/wd2graph">
    </a>
    <a href="https://github.com/cyanic-selkie/wd2graph/blob/main/LICENSE">
        <img alt="License" src="https://img.shields.io/crates/l/wd2graph">
    </a>
    <img alt="Downloads" src="https://shields.io/crates/d/wd2graph">
</p>

## Usage

You can install `wd2graph` by running the following command:

```bash
cargo install wd2graph
```

Of course, you can also build it from source.

`wd2graph` requires only the compressed (`.gz`) Wikidata truthy dump in the N-Triples format as input. You can download it with the following command:

```
wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-truthy.nt.gz
```

After downloading the dump, you can extract the graph data with the following command:
```bash
wd2graph --input latest-truthy.nt.gz \
         --output-graph graph.parquet \
         --output-nodes nodes.parquet
```

The outputs are written into [zstd](https://github.com/facebook/zstd) compressed [Apache Parquet](https://parquet.apache.org/) files.

The file given as the `--output-nodes` argument contains a single column named `qid` (`UInt32`) filled with all of the QIDs.

The file given as the `--output-graph` argument contains 3 columns named `lhs` (`UInt32`), `property` (`UInt32`), and `rhs` (`UInt32`) filled with triplets representing directional edges. `lhs` and `rhs` are the QIDs, while `property` is the PID.    

## Performance

`wd2graph` uses a single thread. On a dump from March 2023, containing \~100,000,000 nodes and \~700,000,000 edges, it takes \~16 minutes to complete with peak memory usage of \~22GB on an AMD Ryzen Threadripper 3970X CPU and an SSD.

