use arrow2::array::UInt32Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::*;
use arrow2::io::parquet::write::{
    transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version, WriteOptions,
};
use clap::Parser;
use linereader::LineReader;
use rust_htslib::bgzf::Reader;
use std::collections::BTreeSet;
use std::fs::File;
use std::str;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the compressed truthy RDF dump of Wikidata.
    #[arg(long)]
    input: String,
    /// Path to the output graph Apache Parquet file.
    #[arg(long)]
    output_graph: String,
    /// Path to the output nodes Apache Parquet file.
    #[arg(long)]
    output_nodes: String,
}

fn main() {
    let args = Args::parse();

    let mut lhs_column = vec![];
    let mut property_column = vec![];
    let mut rhs_column = vec![];

    let mut reader = LineReader::new(Reader::from_path(&args.input).unwrap());
    while let Some(line) = reader.next_line() {
        let line = line.unwrap();
        // LHS
        let pattern = b"<http://www.wikidata.org/entity/Q";
        if !line.starts_with(pattern) {
            continue;
        }

        let i = line[pattern.len()..]
            .into_iter()
            .position(|&x| x == b'>')
            .unwrap();

        let lhs = str::from_utf8(&line[pattern.len()..pattern.len() + i])
            .unwrap()
            .parse::<u32>()
            .unwrap();

        let line = &line[pattern.len() + i + 2..];

        // PROPERTY
        let pattern = b"<http://www.wikidata.org/prop/direct/P";
        if !line.starts_with(pattern) {
            continue;
        }

        let i = line[pattern.len()..]
            .into_iter()
            .position(|&x| x == b'>')
            .unwrap();

        let property = str::from_utf8(&line[pattern.len()..pattern.len() + i])
            .unwrap()
            .parse::<u32>()
            .unwrap();

        let line = &line[pattern.len() + i + 2..];

        // RHS
        let pattern = b"<http://www.wikidata.org/entity/Q";
        if !line.starts_with(pattern) {
            continue;
        }

        let i = line[pattern.len()..]
            .into_iter()
            .position(|&x| x == b'>')
            .unwrap();

        let rhs = str::from_utf8(&line[pattern.len()..pattern.len() + i])
            .unwrap()
            .parse::<u32>()
            .unwrap();

        lhs_column.push(Some(lhs));
        property_column.push(Some(property));
        rhs_column.push(Some(rhs));
    }

    {
        let schema = Schema::from(vec![
            Field::new("lhs", DataType::UInt32, false),
            Field::new("property", DataType::UInt32, false),
            Field::new("rhs", DataType::UInt32, false),
        ]);

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Zstd(None),
            version: Version::V2,
            data_pagesize_limit: None,
        };

        let encodings = schema
            .fields
            .iter()
            .map(|field| transverse(&field.data_type, |_| Encoding::Plain))
            .collect();

        let chunk = Chunk::new(vec![
            UInt32Array::from(&lhs_column).boxed(),
            UInt32Array::from(property_column).boxed(),
            UInt32Array::from(&rhs_column).boxed(),
        ]);

        let row_groups =
            RowGroupIterator::try_new(vec![Ok(chunk)].into_iter(), &schema, options, encodings)
                .unwrap();

        let output_graph = File::create(&args.output_graph).unwrap();
        let mut writer = FileWriter::try_new(output_graph, schema, options).unwrap();

        for group in row_groups {
            writer.write(group.unwrap()).unwrap();
        }

        writer.end(None).unwrap();
    }

    {
        let nodes = BTreeSet::from_iter(lhs_column.into_iter().chain(rhs_column));

        let schema = Schema::from(vec![Field::new("qid", DataType::UInt32, false)]);

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Zstd(None),
            version: Version::V2,
            data_pagesize_limit: None,
        };

        let encodings = schema
            .fields
            .iter()
            .map(|field| transverse(&field.data_type, |_| Encoding::Plain))
            .collect();

        let chunk = Chunk::new(vec![
            UInt32Array::from(Vec::from_iter(nodes.into_iter())).boxed()
        ]);

        let row_groups =
            RowGroupIterator::try_new(vec![Ok(chunk)].into_iter(), &schema, options, encodings)
                .unwrap();

        let output_graph = File::create(&args.output_nodes).unwrap();
        let mut writer = FileWriter::try_new(output_graph, schema, options).unwrap();

        for group in row_groups {
            writer.write(group.unwrap()).unwrap();
        }

        writer.end(None).unwrap();
    }
}
