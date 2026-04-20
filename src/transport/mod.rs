//! Hyperion Distributed Transport Layer
//! 
//! Handles network communication between nodes for distributed query execution,
//! data exchange, and coordination.

use crate::common::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{Bytes, BytesMut};

/// Message types for inter-node communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransportMessage {
    /// Execute a query fragment
    ExecuteQuery {
        query_id: String,
        plan: Vec<u8>,  // Serialized physical plan
        partition_id: usize,
    },
    
    /// Return query results
    QueryResult {
        query_id: String,
        partition_id: usize,
        batches: Vec<ColumnBatch>,
        metrics: QueryMetrics,
    },
    
    /// Send shuffle data
    ShuffleData {
        query_id: String,
        stage_id: usize,
        target_partition: usize,
        batches: Vec<ColumnBatch>,
    },
    
    /// Request for shuffle data
    ShuffleRequest {
        query_id: String,
        stage_id: usize,
        source_partition: usize,
    },
    
    /// Heartbeat between nodes
    Heartbeat {
        node_id: NodeId,
        timestamp_ms: u64,
        load: NodeLoad,
    },
    
    /// Request partition locations
    PartitionLocations {
        table_name: String,
    },
    
    /// Partition location response
    PartitionLocationsResponse {
        table_name: String,
        locations: Vec<PartitionLocation>,
    },
    
    /// Register a node
    RegisterNode {
        node_id: NodeId,
        endpoint: String,
        capabilities: NodeCapabilities,
    },
    
    /// Node registration response
    RegisterNodeResponse {
        node_id: NodeId,
        success: bool,
        cluster_info: Option<ClusterInfo>,
    },
    
    /// Request to abort a query
    AbortQuery {
        query_id: String,
        reason: String,
    },
    
    /// Query aborted acknowledgment
    QueryAborted {
        query_id: String,
    },
    
    /// Open a new connection
    Handshake {
        protocol_version: u32,
        node_id: NodeId,
    },
    
    /// Handshake response
    HandshakeResponse {
        accepted: bool,
        protocol_version: u32,
        error: Option<String>,
    },
    
    /// Error response
    Error {
        code: ErrorCode,
        message: String,
        details: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ErrorCode {
    QueryNotFound,
    NodeNotFound,
    ConnectionFailed,
    SerializationError,
    Timeout,
    ResourceExhausted,
    InternalError,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLoad {
    pub cpu_percent: f64,
    pub memory_percent: f64,
    pub disk_io_percent: f64,
    pub network_io_bytes: u64,
    pub active_queries: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub parallelism: usize,
    pub memory_bytes: u64,
    pub storage_bytes: u64,
    pub supports_gpu: bool,
    pub data_sources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub cluster_id: String,
    pub coordinator_id: NodeId,
    pub nodes: Vec<NodeInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub endpoint: String,
    pub capabilities: NodeCapabilities,
    pub is_coordinator: bool,
    pub is_active: bool,
}

/// Serialized column batch for transport
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedBatch {
    pub num_rows: usize,
    pub columns: Vec<SerializedColumn>,
    pub compression: CompressionType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedColumn {
    pub data: Vec<u8>,
    pub null_mask: Vec<u8>,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Lz4,
    Zstd,
    Gzip,
}

/// Connection pool for node communication
pub struct ConnectionPool {
    connections: RwLock<HashMap<NodeId, ConnectionPoolEntry>>,
    config: PoolConfig,
}

struct ConnectionPoolEntry {
    connection: Box<dyn TransportConnection>,
    last_used_ms: u64,
    in_use: bool,
}

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_connections_per_node: usize,
    pub connection_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub max_retries: u32,
    pub retry_delay_ms: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_node: 10,
            connection_timeout_ms: 5000,
            idle_timeout_ms: 300000,
            max_retries: 3,
            retry_delay_ms: 100,
        }
    }
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            config,
        }
    }

    pub async fn get_connection(&self, node_id: &NodeId, endpoint: &str) -> Result<Box<dyn TransportConnection>> {
        let mut connections = self.connections.write().await;
        
        // Check existing connections
        if let Some(entry) = connections.get_mut(node_id) {
            if !entry.in_use {
                entry.in_use = true;
                entry.last_used_ms = current_time_ms();
                return Ok(entry.connection.box_clone());
            }
        }
        
        // Create new connection
        let conn = TcpTransportConnection::connect(endpoint).await?;
        
        connections.insert(node_id.clone(), ConnectionPoolEntry {
            connection: Box::new(conn),
            last_used_ms: current_time_ms(),
            in_use: true,
        });
        
        // Return cloned connection
        let entry = connections.get_mut(node_id).unwrap();
        Ok(entry.connection.box_clone())
    }

    pub async fn release_connection(&self, node_id: &NodeId) {
        let mut connections = self.connections.write().await;
        if let Some(entry) = connections.get_mut(node_id) {
            entry.in_use = false;
        }
    }

    pub async fn close_all(&self) {
        let mut connections = self.connections.write().await;
        for (_, entry) in connections.drain() {
            let _ = entry.connection.close().await;
        }
    }
}

/// Trait for transport connections
pub trait TransportConnection: Send {
    fn send(&mut self, message: &TransportMessage) -> impl std::future::Future<Output = Result<()>> + Send;
    fn receive(&mut self) -> impl std::future::Future<Output = Result<TransportMessage>> + Send;
    fn close(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;
    fn box_clone(&self) -> Box<dyn TransportConnection>;
}

/// TCP-based transport connection
pub struct TcpTransportConnection {
    stream: TcpStream,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
}

impl TcpTransportConnection {
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let stream = TcpStream::connect(endpoint).await
            .map_err(|e| HyperionError::Network(format!("Failed to connect to {}: {}", endpoint, e)))?;
        
        Ok(Self {
            stream,
            read_buffer: BytesMut::with_capacity(65536),
            write_buffer: BytesMut::with_capacity(65536),
        })
    }
}

impl TransportConnection for TcpTransportConnection {
    async fn send(&mut self, message: &TransportMessage) -> Result<()> {
        // Serialize message
        let data = serde_json::to_vec(message)
            .map_err(|e| HyperionError::Serialization(e.to_string()))?;
        
        // Write length prefix
        let len = (data.len() as u32).to_be_bytes();
        self.stream.write_all(&len).await
            .map_err(|e| HyperionError::Network(e.to_string()))?;
        
        // Write data
        self.stream.write_all(&data).await
            .map_err(|e| HyperionError::Network(e.to_string()))?;
        
        Ok(())
    }

    async fn receive(&mut self) -> Result<TransportMessage> {
        // Read length prefix
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await
            .map_err(|e| HyperionError::Network(e.to_string()))?;
        
        let len = u32::from_be_bytes(len_buf) as usize;
        
        // Read data
        self.read_buffer.resize(len, 0);
        self.stream.read_exact(&mut self.read_buffer).await
            .map_err(|e| HyperionError::Network(e.to_string()))?;
        
        // Deserialize
        let message: TransportMessage = serde_json::from_slice(&self.read_buffer)
            .map_err(|e| HyperionError::Serialization(e.to_string()))?;
        
        Ok(message)
    }

    async fn close(&mut self) -> Result<()> {
        self.stream.shutdown().await
            .map_err(|e| HyperionError::Network(e.to_string()))?;
        Ok(())
    }

    fn box_clone(&self) -> Box<dyn TransportConnection> {
        Box::new(TcpTransportConnection {
            stream: TcpStream::connect("dummy").await.unwrap(),
            read_buffer: BytesMut::new(),
            write_buffer: BytesMut::new(),
        })
    }
}

/// Distributed query scheduler
pub struct QueryScheduler {
    node_manager: Arc<NodeManager>,
    config: SchedulerConfig,
}

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub schedule_timeout_ms: u64,
    pub max_concurrent_queries: usize,
    pub load_balancing: LoadBalancingStrategy,
    pub speculative_execution: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            schedule_timeout_ms: 10000,
            max_concurrent_queries: 100,
            load_balancing: LoadBalancingStrategy::LeastLoaded,
            speculative_execution: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum LoadBalancingStrategy {
    RoundRobin,
    LeastLoaded,
    Random,
    LocalFirst,
    DataLocality,
}

impl QueryScheduler {
    pub fn new(node_manager: Arc<NodeManager>, config: SchedulerConfig) -> Self {
        Self { node_manager, config }
    }

    /// Schedule query fragments across nodes
    pub async fn schedule_query(
        &self,
        fragments: Vec<QueryFragment>,
        locations: &HashMap<String, Vec<PartitionLocation>>,
    ) -> Result<ScheduleResult> {
        let mut assignments = HashMap::new();
        
        for fragment in fragments {
            // Find best node for this fragment
            let node = self.select_node(&fragment, locations).await?;
            
            assignments.insert(fragment.id.clone(), NodeAssignment {
                node_id: node.node_id.clone(),
                endpoint: node.endpoint.clone(),
                fragments: vec![fragment.id.clone()],
                estimated_cost: fragment.estimated_cost,
            });
        }
        
        Ok(ScheduleResult { assignments })
    }

    async fn select_node(&self, fragment: &QueryFragment, _locations: &HashMap<String, Vec<PartitionLocation>>) -> Result<NodeInfo> {
        match self.config.load_balancing {
            LoadBalancingStrategy::LeastLoaded => {
                // Find node with lowest load
                let nodes = self.node_manager.get_active_nodes().await;
                nodes.into_iter()
                    .min_by_key(|n| n.capabilities.parallelism)
                    .ok_or_else(|| HyperionError::Network("No available nodes".into()))
            }
            LoadBalancingStrategy::LocalFirst => {
                // Prefer nodes with local data
                Ok(self.node_manager.get_coordinator().await?)
            }
            _ => {
                Ok(self.node_manager.get_coordinator().await?)
            }
        }
    }
}

/// A fragment of a distributed query
#[derive(Debug, Clone)]
pub struct QueryFragment {
    pub id: String,
    pub stage_id: usize,
    pub partitions: usize,
    pub estimated_cost: f64,
    pub input_partitions: Vec<String>,
    pub partitioning_scheme: PartitioningScheme,
    pub required_columns: Vec<String>,
    pub predicates: Vec<crate::planner::Expression>,
}

/// Result of scheduling
#[derive(Debug, Clone)]
pub struct ScheduleResult {
    pub assignments: HashMap<String, NodeAssignment>,
}

/// Assignment of fragments to a node
#[derive(Debug, Clone)]
pub struct NodeAssignment {
    pub node_id: NodeId,
    pub endpoint: String,
    pub fragments: Vec<String>,
    pub estimated_cost: f64,
}

/// Node manager for tracking cluster state
pub struct NodeManager {
    nodes: RwLock<HashMap<NodeId, NodeState>>,
    coordinator_id: RwLock<NodeId>,
    config: NodeManagerConfig,
}

struct NodeState {
    info: NodeInfo,
    load: NodeLoad,
    last_heartbeat_ms: u64,
    partitions: Vec<PartitionInfo>,
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub table_name: String,
    pub partition_id: PartitionId,
    pub row_count: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct NodeManagerConfig {
    pub heartbeat_interval_ms: u64,
    pub node_timeout_ms: u64,
    pub max_nodes: usize,
}

impl Default for NodeManagerConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 5000,
            node_timeout_ms: 30000,
            max_nodes: 1000,
        }
    }
}

impl NodeManager {
    pub fn new(coordinator_id: NodeId, config: NodeManagerConfig) -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            coordinator_id: RwLock::new(coordinator_id),
            config,
        }
    }

    /// Register a new node
    pub async fn register_node(&self, info: NodeInfo) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        
        if nodes.len() >= self.config.max_nodes {
            return Err(HyperionError::Network("Maximum nodes reached".into()));
        }
        
        nodes.insert(info.node_id.clone(), NodeState {
            info: info.clone(),
            load: NodeLoad {
                cpu_percent: 0.0,
                memory_percent: 0.0,
                disk_io_percent: 0.0,
                network_io_bytes: 0,
                active_queries: 0,
            },
            last_heartbeat_ms: current_time_ms(),
            partitions: vec![],
        });
        
        Ok(())
    }

    /// Update node load from heartbeat
    pub async fn update_heartbeat(&self, node_id: &NodeId, load: NodeLoad) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        
        if let Some(state) = nodes.get_mut(node_id) {
            state.load = load;
            state.last_heartbeat_ms = current_time_ms();
            Ok(())
        } else {
            Err(HyperionError::NotFound(format!("Node {} not found", node_id.0)))
        }
    }

    /// Get all active nodes
    pub async fn get_active_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        let now = current_time_ms();
        
        nodes.values()
            .filter(|state| {
                now - state.last_heartbeat_ms < self.config.node_timeout_ms
            })
            .map(|state| state.info.clone())
            .collect()
    }

    /// Get coordinator node
    pub async fn get_coordinator(&self) -> Result<NodeInfo> {
        let coordinator_id = self.coordinator_id.read().await;
        let nodes = self.nodes.read().await;
        
        nodes.get(&*coordinator_id)
            .map(|state| state.info.clone())
            .ok_or_else(|| HyperionError::NotFound("Coordinator not found".into()))
    }

    /// Get nodes containing specific partitions
    pub async fn get_nodes_for_partitions(&self, table: &str, partitions: &[PartitionId]) -> Result<Vec<NodeInfo>> {
        let nodes = self.nodes.read().await;
        
        let result: Vec<NodeInfo> = nodes.values()
            .filter(|state| {
                state.partitions.iter().any(|p| {
                    p.table_name == table && partitions.contains(&p.partition_id)
                })
            })
            .map(|state| state.info.clone())
            .collect();
        
        if result.is_empty() {
            // Fall back to any active node
            Ok(self.get_active_nodes().await)
        } else {
            Ok(result)
        }
    }

    /// Remove inactive nodes
    pub async fn remove_inactive_nodes(&self) {
        let mut nodes = self.nodes.write().await;
        let now = current_time_ms();
        
        nodes.retain(|_, state| {
            now - state.last_heartbeat_ms < self.config.node_timeout_ms
        });
    }
}

/// Data exchange manager for shuffle operations
pub struct ShuffleManager {
    shuffle_cache: RwLock<HashMap<ShuffleKey, ShuffleBuffer>>,
    config: ShuffleConfig,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ShuffleKey {
    pub query_id: String,
    pub stage_id: usize,
    pub partition_id: usize,
}

#[derive(Debug, Clone)]
pub struct ShuffleBuffer {
    pub data: Vec<ColumnBatch>,
    pub bytes_written: u64,
    pub created_at_ms: u64,
    pub reference_count: usize,
}

#[derive(Debug, Clone)]
pub struct ShuffleConfig {
    pub memory_limit_mb: u64,
    pub spill_directory: Option<String>,
    pub compression: CompressionType,
    pub block_size_mb: u64,
}

impl Default for ShuffleConfig {
    fn default() -> Self {
        Self {
            memory_limit_mb: 512,
            spill_directory: None,
            compression: CompressionType::Lz4,
            block_size_mb: 64,
        }
    }
}

impl ShuffleManager {
    pub fn new(config: ShuffleConfig) -> Self {
        Self {
            shuffle_cache: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Register a shuffle producer
    pub async fn register_producer(&self, key: ShuffleKey) -> ShuffleWriter {
        ShuffleWriter {
            key,
            manager: self.clone(),
            buffer: Vec::new(),
            bytes_written: 0,
        }
    }

    /// Register a shuffle consumer
    pub async fn register_consumer(&self, key: ShuffleKey) -> Result<ShuffleReader> {
        let buffer = {
            let cache = self.shuffle_cache.read().await;
            cache.get(&key).cloned()
        };
        
        match buffer {
            Some(buf) => {
                let mut cache = self.shuffle_cache.write().await;
                if let Some(entry) = cache.get_mut(&key) {
                    entry.reference_count += 1;
                }
                
                Ok(ShuffleReader {
                    key,
                    buffer: Some(buf.data),
                    position: 0,
                    manager: self.clone(),
                })
            }
            None => Err(HyperionError::NotFound("Shuffle data not found".into())),
        }
    }

    /// Complete a shuffle write
    async fn complete_write(&self, key: ShuffleKey, data: Vec<ColumnBatch>, bytes: u64) {
        let mut cache = self.shuffle_cache.write().await;
        
        cache.insert(key, ShuffleBuffer {
            data,
            bytes_written: bytes,
            created_at_ms: current_time_ms(),
            reference_count: 1,
        });
    }

    /// Release a shuffle buffer
    async fn release(&self, key: &ShuffleKey) {
        let mut cache = self.shuffle_cache.write().await;
        
        if let Some(entry) = cache.get_mut(key) {
            entry.reference_count = entry.reference_count.saturating_sub(1);
            
            if entry.reference_count == 0 {
                cache.remove(key);
            }
        }
    }
}

/// Writer for shuffle data
pub struct ShuffleManager;

#[derive(Clone)]
pub struct ShuffleWriter {
    key: ShuffleKey,
    manager: ShuffleManager,
    buffer: Vec<ColumnBatch>,
    bytes_written: u64,
}

impl ShuffleWriter {
    /// Write a batch to the shuffle buffer
    pub async fn write(&mut self, batch: ColumnBatch) {
        let batch_size = batch.num_rows * 100;  // Estimate
        self.bytes_written += batch_size as u64;
        self.buffer.push(batch);
    }

    /// Close and finalize the shuffle write
    pub async fn close(self) -> Result<()> {
        self.manager.complete_write(self.key, self.buffer, self.bytes_written).await;
        Ok(())
    }
}

/// Reader for shuffle data
pub struct ShuffleReader {
    key: ShuffleKey,
    buffer: Option<Vec<ColumnBatch>>,
    position: usize,
    manager: ShuffleManager,
}

impl ShuffleReader {
    /// Read next batch
    pub fn next_batch(&mut self) -> Option<ColumnBatch> {
        if let Some(ref mut buffer) = self.buffer {
            if self.position < buffer.len() {
                let batch = buffer[self.position].clone();
                self.position += 1;
                return Some(batch);
            }
        }
        None
    }

    /// Close and release resources
    pub async fn close(self) {
        self.manager.release(&self.key).await;
    }
}

// Utility functions

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

use crate::storage::ColumnBatch;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_manager() {
        let manager = NodeManager::new(
            NodeId("coordinator".into()),
            NodeManagerConfig::default(),
        );
        
        // Register a node
        let node_info = NodeInfo {
            node_id: NodeId("worker-1".into()),
            endpoint: "localhost:8080".into(),
            capabilities: NodeCapabilities {
                parallelism: 4,
                memory_bytes: 1024 * 1024 * 1024,
                storage_bytes: 100 * 1024 * 1024 * 1024,
                supports_gpu: false,
                data_sources: vec![],
            },
            is_coordinator: false,
            is_active: true,
        };
        
        manager.register_node(node_info).await.unwrap();
        
        // Get active nodes
        let nodes = manager.get_active_nodes().await;
        assert_eq!(nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_connection_pool() {
        let pool = ConnectionPool::new(PoolConfig::default());
        // Test connection management
    }

    #[test]
    fn test_shuffle_key() {
        let key1 = ShuffleKey {
            query_id: "q1".into(),
            stage_id: 1,
            partition_id: 0,
        };
        
        let key2 = ShuffleKey {
            query_id: "q1".into(),
            stage_id: 1,
            partition_id: 0,
        };
        
        assert_eq!(key1, key2);
    }
}
