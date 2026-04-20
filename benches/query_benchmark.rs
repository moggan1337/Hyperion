//! Benchmarks for Hyperion query engine

use hyperion::*;
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_simple_scan(c: &mut Criterion) {
    let config = EngineConfig::default();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    
    runtime.block_on(async {
        let engine = HyperionEngine::new(config);
        
        // Register test table
        let schema = Schema::new("bench_table".into(), vec![
            Column {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
                ordinal: 0,
            },
            Column {
                name: "value".into(),
                data_type: DataType::Varchar(256),
                nullable: true,
                ordinal: 1,
            },
        ]);
        
        let table = DistributedTable {
            name: "bench_table".into(),
            schema,
            partitions: vec![],
            partitioning_scheme: PartitioningScheme::RoundRobin,
        };
        
        engine.register_table(table).unwrap();
        
        c.bench_function("simple_scan", |b| {
            b.to_async(&runtime).iter(|| async {
                engine.execute(black_box("SELECT * FROM bench_table")).await
            });
        });
    });
}

fn bench_aggregation(c: &mut Criterion) {
    let config = EngineConfig::default();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    
    runtime.block_on(async {
        let engine = HyperionEngine::new(config);
        
        let schema = Schema::new("sales".into(), vec![
            Column {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
                ordinal: 0,
            },
            Column {
                name: "amount".into(),
                data_type: DataType::Double,
                nullable: false,
                ordinal: 1,
            },
            Column {
                name: "category".into(),
                data_type: DataType::Varchar(50),
                nullable: false,
                ordinal: 2,
            },
        ]);
        
        let table = DistributedTable {
            name: "sales".into(),
            schema,
            partitions: vec![],
            partitioning_scheme: PartitioningScheme::RoundRobin,
        };
        
        engine.register_table(table).unwrap();
        
        c.bench_function("aggregation_sum", |b| {
            b.to_async(&runtime).iter(|| async {
                engine.execute(black_box(
                    "SELECT category, SUM(amount), COUNT(*) FROM sales GROUP BY category"
                )).await
            });
        });
    });
}

fn bench_join(c: &mut Criterion) {
    let config = EngineConfig::default();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    
    runtime.block_on(async {
        let engine = HyperionEngine::new(config);
        
        // Register tables for join
        let left_schema = Schema::new("left_table".into(), vec![
            Column {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
                ordinal: 0,
            },
            Column {
                name: "value".into(),
                data_type: DataType::Double,
                nullable: false,
                ordinal: 1,
            },
        ]);
        
        let right_schema = Schema::new("right_table".into(), vec![
            Column {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
                ordinal: 0,
            },
            Column {
                name: "name".into(),
                data_type: DataType::Varchar(100),
                nullable: false,
                ordinal: 1,
            },
        ]);
        
        engine.register_table(DistributedTable {
            name: "left_table".into(),
            schema: left_schema,
            partitions: vec![],
            partitioning_scheme: PartitioningScheme::RoundRobin,
        }).unwrap();
        
        engine.register_table(DistributedTable {
            name: "right_table".into(),
            schema: right_schema,
            partitions: vec![],
            partitioning_scheme: PartitioningScheme::RoundRobin,
        }).unwrap();
        
        c.bench_function("hash_join", |b| {
            b.to_async(&runtime).iter(|| async {
                engine.execute(black_box(
                    "SELECT l.id, l.value, r.name FROM left_table l JOIN right_table r ON l.id = r.id"
                )).await
            });
        });
    });
}

criterion_group! {
    benches,
    bench_simple_scan,
    bench_aggregation,
    bench_join
}

criterion_main!(benches);
