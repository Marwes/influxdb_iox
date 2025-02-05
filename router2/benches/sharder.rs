use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion, Throughput};
use data_types::DatabaseName;
use router2::sharder::{Sharder, TableNamespaceSharder};

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

fn get_random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

fn sharder_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("sharder");

    // benchmark sharder with fixed table name and namespace, with varying number of buckets
    benchmark_sharder(
        &mut group,
        1_000,
        "basic 1k buckets",
        "table",
        &DatabaseName::try_from("namespace").unwrap(),
    );
    benchmark_sharder(
        &mut group,
        10_000,
        "basic 10k buckets",
        "table",
        &DatabaseName::try_from("namespace").unwrap(),
    );
    benchmark_sharder(
        &mut group,
        100_000,
        "basic 100k buckets",
        "table",
        &DatabaseName::try_from("namespace").unwrap(),
    );
    benchmark_sharder(
        &mut group,
        1_000_000,
        "basic 1M buckets",
        "table",
        &DatabaseName::try_from("namespace").unwrap(),
    );

    // benchmark sharder with random table name and namespace of length 16
    benchmark_sharder(
        &mut group,
        10_000,
        "random with key-length 16",
        get_random_string(16).as_str(),
        &DatabaseName::try_from(get_random_string(16)).unwrap(),
    );

    // benchmark sharder with random table name and namespace of length 32
    benchmark_sharder(
        &mut group,
        10_000,
        "random with key-length 32",
        get_random_string(32).as_str(),
        &DatabaseName::try_from(get_random_string(32)).unwrap(),
    );

    // benchmark sharder with random table name and namespace of length 64
    benchmark_sharder(
        &mut group,
        10_000,
        "random with key-length 64",
        get_random_string(64).as_str(),
        &DatabaseName::try_from(get_random_string(64)).unwrap(),
    );

    group.finish();
}

fn benchmark_sharder(
    group: &mut BenchmarkGroup<WallTime>,
    num_buckets: usize,
    bench_name: &str,
    table: &str,
    namespace: &DatabaseName<'_>,
) {
    let hasher = TableNamespaceSharder::new(0..num_buckets);

    group.throughput(Throughput::Elements(1));
    group.bench_function(bench_name, |b| {
        b.iter(|| {
            hasher.shard(table, namespace, &0);
        });
    });
}

criterion_group!(benches, sharder_benchmarks);
criterion_main!(benches);
