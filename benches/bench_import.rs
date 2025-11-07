use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

fn benchmark_data_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_generation");
    group.throughput(Throughput::Elements(10000));

    group.bench_function("generate_rows", |b| {
        b.iter(|| {
            let rows: Vec<(i64, String, f64)> = (0..10000)
                .map(|i| {
                    (
                        black_box(i),
                        black_box(format!("value_{}", i)),
                        black_box(i as f64 * 1.5),
                    )
                })
                .collect();
            black_box(rows)
        });
    });

    group.finish();
}

fn benchmark_compression(c: &mut Criterion) {
    let data: Vec<i64> = (0..10000).collect();
    let bytes: Vec<u8> = data.iter().flat_map(|x| x.to_le_bytes()).collect();

    let mut group = c.benchmark_group("compression");
    group.throughput(Throughput::Bytes(bytes.len() as u64));

    group.bench_function("lz4_compress", |b| {
        b.iter(|| {
            let compressed = lz4_flex::compress_prepend_size(&bytes);
            black_box(compressed)
        });
    });

    group.bench_function("lz4_decompress", |b| {
        let compressed = lz4_flex::compress_prepend_size(&bytes);
        b.iter(|| {
            let decompressed = lz4_flex::decompress_size_prepended(&compressed).unwrap();
            black_box(decompressed)
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_data_generation, benchmark_compression);
criterion_main!(benches);
