//! Integration tests for Hyperion query engine

use hyperion::*;

#[tokio::test]
async fn test_simple_scan() {
    let config = EngineConfig::default();
    let engine = HyperionEngine::new(config);
    
    // Register a test table
    let schema = Schema::new("test_users".into(), vec![
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
        Column {
            name: "age".into(),
            data_type: DataType::Integer,
            nullable: true,
            ordinal: 2,
        },
    ]);
    
    let table = DistributedTable {
        name: "test_users".into(),
        schema,
        partitions: vec![],
        partitioning_scheme: PartitioningScheme::RoundRobin,
    };
    
    engine.register_table(table).unwrap();
    
    // Execute a simple query
    let result = engine.execute("SELECT id, name FROM test_users").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_aggregation_query() {
    let config = EngineConfig::default();
    let engine = HyperionEngine::new(config);
    
    // Register table with test data
    let schema = Schema::new("orders".into(), vec![
        Column {
            name: "order_id".into(),
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
            name: "customer_id".into(),
            data_type: DataType::Integer,
            nullable: false,
            ordinal: 2,
        },
    ]);
    
    let table = DistributedTable {
        name: "orders".into(),
        schema,
        partitions: vec![],
        partitioning_scheme: PartitioningScheme::Hash {
            column: "customer_id".into(),
            num_partitions: 4,
        },
    };
    
    engine.register_table(table).unwrap();
    
    // Execute aggregation query
    let result = engine.execute(
        "SELECT customer_id, SUM(amount), COUNT(*) FROM orders GROUP BY customer_id"
    ).await;
    
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_join_query() {
    let config = EngineConfig::default();
    let engine = HyperionEngine::new(config);
    
    // Register customers table
    let customers_schema = Schema::new("customers".into(), vec![
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
    
    let customers = DistributedTable {
        name: "customers".into(),
        schema: customers_schema,
        partitions: vec![],
        partitioning_scheme: PartitioningScheme::RoundRobin,
    };
    
    engine.register_table(customers).unwrap();
    
    // Register orders table
    let orders_schema = Schema::new("orders".into(), vec![
        Column {
            name: "id".into(),
            data_type: DataType::Integer,
            nullable: false,
            ordinal: 0,
        },
        Column {
            name: "customer_id".into(),
            data_type: DataType::Integer,
            nullable: false,
            ordinal: 1,
        },
        Column {
            name: "total".into(),
            data_type: DataType::Double,
            nullable: false,
            ordinal: 2,
        },
    ]);
    
    let orders = DistributedTable {
        name: "orders".into(),
        schema: orders_schema,
        partitions: vec![],
        partitioning_scheme: PartitioningScheme::Hash {
            column: "customer_id".into(),
            num_partitions: 4,
        },
    };
    
    engine.register_table(orders).unwrap();
    
    // Execute join query
    let result = engine.execute(
        "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id"
    ).await;
    
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_filter_query() {
    let config = EngineConfig::default();
    let engine = HyperionEngine::new(config);
    
    let schema = Schema::new("users".into(), vec![
        Column {
            name: "id".into(),
            data_type: DataType::Integer,
            nullable: false,
            ordinal: 0,
        },
        Column {
            name: "age".into(),
            data_type: DataType::Integer,
            nullable: true,
            ordinal: 1,
        },
    ]);
    
    let table = DistributedTable {
        name: "users".into(),
        schema,
        partitions: vec![],
        partitioning_scheme: PartitioningScheme::RoundRobin,
    };
    
    engine.register_table(table).unwrap();
    
    // Execute filtered query
    let result = engine.execute(
        "SELECT * FROM users WHERE age > 18 AND age < 65"
    ).await;
    
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_limit_query() {
    let config = EngineConfig::default();
    let engine = HyperionEngine::new(config);
    
    let schema = Schema::new("products".into(), vec![
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
    
    let table = DistributedTable {
        name: "products".into(),
        schema,
        partitions: vec![],
        partitioning_scheme: PartitioningScheme::RoundRobin,
    };
    
    engine.register_table(table).unwrap();
    
    // Execute query with limit
    let result = engine.execute(
        "SELECT * FROM products ORDER BY id LIMIT 10"
    ).await;
    
    assert!(result.is_ok());
}
