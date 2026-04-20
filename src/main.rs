//! Hyperion - Distributed SQL Query Engine
//! 
//! This is the main binary entry point for the Hyperion query engine.
//! It provides both single-node and distributed modes of operation.

use hyperion::{HyperionEngine, EngineConfig, ClusterConfig, Config};
use std::env;
use std::path::PathBuf;
use clap::{Parser, Subcommand};

/// Hyperion CLI arguments
#[derive(Parser, Debug)]
#[command(name = "hyperion")]
#[command(about = "Hyperion - Distributed SQL Query Engine", long_about = None)]
struct Args {
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
    
    /// Configuration file path
    #[arg(short, long)]
    config: Option<PathBuf>,
    
    /// Port to listen on
    #[arg(short, long, default_value = "8080")]
    port: u16,
    
    /// Enable distributed mode
    #[arg(short, long)]
    distributed: bool,
    
    /// Coordinator endpoint for distributed mode
    #[arg(long, requires = "distributed")]
    coordinator: Option<String>,
    
    /// Nodes for cluster (comma-separated)
    #[arg(long, requires = "distributed")]
    nodes: Option<String>,
    
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Execute a SQL query
    Query {
        /// SQL query to execute
        sql: String,
    },
    /// Start the query server
    Server {
        /// SQL file to execute on startup
        #[arg(short, long)]
        init: Option<PathBuf>,
    },
    /// Run benchmark
    Bench {
        /// Number of iterations
        #[arg(short, long, default_value = "10")]
        iterations: usize,
        /// Query to benchmark
        query: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    // Initialize logging
    env_logger::init();
    
    if args.distributed {
        run_distributed(args).await?;
    } else {
        run_single_node(args).await?;
    }
    
    Ok(())
}

async fn run_single_node(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let config = EngineConfig::default();
    let engine = HyperionEngine::new(config);
    
    match args.command {
        Some(Commands::Query { sql }) => {
            println!("Executing: {}", sql);
            let result = engine.execute(&sql).await?;
            println!("\nResults:");
            for batch in result.batches {
                println!("{:?}", batch);
            }
            println!("\nMetrics: {:?}", result.metrics);
        }
        Some(Commands::Server { init }) => {
            println!("Starting Hyperion server on port {}", args.port);
            
            if let Some(init_file) = init {
                let sql = std::fs::read_to_string(&init_file)?;
                for statement in sql.split(';') {
                    let statement = statement.trim();
                    if !statement.is_empty() {
                        println!("Executing: {}", statement);
                        let _ = engine.execute(statement).await;
                    }
                }
            }
            
            println!("Server running. Press Ctrl+C to stop.");
            tokio::signal::ctrl_c().await?;
            println!("\nShutting down...");
        }
        Some(Commands::Bench { iterations, query }) => {
            println!("Running benchmark: {}", query);
            let mut times = Vec::new();
            
            for i in 0..iterations {
                let start = std::time::Instant::now();
                let _ = engine.execute(&query).await;
                let elapsed = start.elapsed();
                times.push(elapsed);
                println!("Iteration {}: {:?}", i + 1, elapsed);
            }
            
            let avg = times.iter().sum::<std::time::Duration>() / times.len() as u32;
            println!("\nAverage: {:?}", avg);
        }
        None => {
            println!("Hyperion SQL Engine");
            println!("Use --help for usage information");
        }
    }
    
    Ok(())
}

async fn run_distributed(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let coordinator = args.coordinator.unwrap_or_else(|| "localhost:8080".to_string());
    
    let mut cluster = ClusterConfig::new()
        .set_coordinator(&coordinator);
    
    if let Some(nodes) = args.nodes {
        for node in nodes.split(',') {
            cluster = cluster.add_node(node.trim());
        }
    }
    
    let config = EngineConfig::default();
    let engine = HyperionEngine::distributed(config, cluster).await?;
    
    match args.command {
        Some(Commands::Query { sql }) => {
            println!("Executing distributed query: {}", sql);
            let result = engine.execute(&sql).await?;
            println!("\nResults:");
            for batch in result.batches {
                println!("{:?}", batch);
            }
        }
        Some(Commands::Server { .. }) => {
            println!("Starting distributed Hyperion node, coordinator: {}", coordinator);
            println!("Node listening on port {}", args.port);
            tokio::signal::ctrl_c().await?;
        }
        _ => {}
    }
    
    Ok(())
}
