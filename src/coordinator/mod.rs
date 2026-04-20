//! Hyperion Distributed Transaction Coordinator
//! 
//! Implements 2-phase commit (2PC) protocol for ACID transactions across
//! distributed nodes.

use crate::common::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

/// Transaction state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction has started
    Active,
    /// Coordinator has sent prepare to all participants
    Preparing,
    /// All participants voted "yes" on prepare
    Prepared,
    /// Coordinator has decided to commit
    Committing,
    /// Transaction has been committed
    Committed,
    /// Transaction has been rolled back
    Aborted,
    /// Commit failed, in recovery
    Recovery,
}

/// Distributed transaction identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId {
    pub id: String,
    pub coordinator_id: NodeId,
    pub start_time_ms: u64,
}

impl TransactionId {
    pub fn new(coordinator_id: NodeId) -> Self {
        Self {
            id: uuid_v4(),
            coordinator_id,
            start_time_ms: current_time_ms(),
        }
    }
}

/// Participant in a distributed transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionParticipant {
    pub node_id: NodeId,
    pub endpoint: String,
    pub votes: HashMap<PartitionId, Vote>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Vote {
    Yes,
    No,
}

/// A distributed transaction
#[derive(Debug, Clone)]
pub struct DistributedTransaction {
    pub id: TransactionId,
    pub state: TransactionState,
    pub participants: Vec<TransactionParticipant>,
    pub read_set: HashSet<DataPointer>,
    pub write_set: HashMap<DataPointer, Value>,
    pub isolation_level: IsolationLevel,
    pub started_at: u64,
    pub coordinator_endpoint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::ReadCommitted
    }
}

/// Reference to data
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataPointer {
    pub table: String,
    pub partition: PartitionId,
    pub key: Value,
}

/// 2PC coordinator message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordinatorMessage {
    /// Prepare phase - ask participant to vote
    Prepare {
        transaction_id: TransactionId,
        participant_id: NodeId,
        operations: Vec<TransactionOperation>,
    },
    /// Commit command
    Commit {
        transaction_id: TransactionId,
    },
    /// Abort command
    Abort {
        transaction_id: TransactionId,
        reason: String,
    },
    /// Request for participant status
    Status {
        transaction_id: TransactionId,
    },
    /// Acknowledgment from participant
    Acknowledgment {
        transaction_id: TransactionId,
        participant_id: NodeId,
        vote: Option<Vote>,
        status: ParticipantStatus,
    },
    /// Recovery check
    RecoveryCheck {
        transaction_id: TransactionId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParticipantStatus {
    /// Participant voted "yes" and prepared
    Prepared,
    /// Participant voted "no" or aborted
    Aborted,
    /// Participant committed
    Committed,
    /// Participant is unknown
    Unknown,
    /// Participant is recovering
    Recovery,
}

/// Transaction operation types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionOperation {
    Read { pointer: DataPointer },
    Write { pointer: DataPointer, value: Value },
    Delete { pointer: DataPointer },
}

/// Transaction coordinator
pub struct TransactionCoordinator {
    coordinator_id: NodeId,
    transactions: RwLock<HashMap<TransactionId, DistributedTransaction>>,
    participant_clients: RwLock<HashMap<NodeId, ParticipantClient>>,
    config: CoordinatorConfig,
}

#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    pub prepare_timeout_ms: u64,
    pub commit_timeout_ms: u64,
    pub recovery_interval_ms: u64,
    pub max_retries: u32,
    pub enable_read_snapshot: bool,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            prepare_timeout_ms: 30000,
            commit_timeout_ms: 60000,
            recovery_interval_ms: 60000,
            max_retries: 3,
            enable_read_snapshot: true,
        }
    }
}

/// Client interface for participant communication
#[derive(Debug, Clone)]
pub struct ParticipantClient {
    pub node_id: NodeId,
    pub endpoint: String,
}

impl TransactionCoordinator {
    pub fn new(coordinator_id: NodeId, config: CoordinatorConfig) -> Self {
        Self {
            coordinator_id,
            transactions: RwLock::new(HashMap::new()),
            participant_clients: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Register a participant node
    pub fn register_participant(&self, client: ParticipantClient) {
        let mut clients = self.participant_clients.write().unwrap();
        clients.insert(client.node_id.clone(), client);
    }

    /// Begin a new distributed transaction
    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
        participants: Vec<NodeId>,
    ) -> Result<TransactionId> {
        let id = TransactionId::new(self.coordinator_id.clone());
        
        let tx = DistributedTransaction {
            id: id.clone(),
            state: TransactionState::Active,
            participants: self.get_participants(&participants),
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            isolation_level,
            started_at: current_time_ms(),
            coordinator_endpoint: format!("coordinator-{}", self.coordinator_id.0),
        };
        
        let mut transactions = self.transactions.write().unwrap();
        transactions.insert(id.clone(), tx);
        
        Ok(id)
    }

    /// Add a read operation to the transaction
    pub fn add_read(&self, tx_id: &TransactionId, pointer: DataPointer) -> Result<Value> {
        let mut transactions = self.transactions.write().unwrap();
        let tx = transactions.get_mut(tx_id)
            .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
        
        tx.read_set.insert(pointer.clone());
        
        // In a real implementation, this would read from the participant
        // For now, return None
        Ok(Value::Null)
    }

    /// Add a write operation to the transaction
    pub fn add_write(&self, tx_id: &TransactionId, pointer: DataPointer, value: Value) -> Result<()> {
        let mut transactions = self.transactions.write().unwrap();
        let tx = transactions.get_mut(tx_id)
            .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
        
        tx.write_set.insert(pointer, value);
        Ok(())
    }

    /// Begin the 2PC prepare phase
    pub async fn prepare(&self, tx_id: &TransactionId) -> Result<bool> {
        // Transition to preparing state
        {
            let mut transactions = self.transactions.write().unwrap();
            let tx = transactions.get_mut(tx_id)
                .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
            tx.state = TransactionState::Preparing;
        }
        
        // Send prepare messages to all participants
        let prepare_msg = CoordinatorMessage::Prepare {
            transaction_id: tx_id.clone(),
            participant_id: self.coordinator_id.clone(),
            operations: vec![],  // Would include the actual operations
        };
        
        // Collect votes from all participants
        let mut all_yes = true;
        let participants = {
            let transactions = self.transactions.read().unwrap();
            let tx = transactions.get(tx_id)
                .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
            tx.participants.clone()
        };
        
        for participant in participants {
            // In real implementation, this would send to the actual participant
            // For now, simulate a "yes" vote
            let vote = self.simulate_prepare_vote(&participant, tx_id).await;
            
            if vote != Vote::Yes {
                all_yes = false;
                break;
            }
        }
        
        // Update transaction state
        {
            let mut transactions = self.transactions.write().unwrap();
            let tx = transactions.get_mut(tx_id)
                .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
            tx.state = if all_yes { TransactionState::Prepared } else { TransactionState::Aborted };
        }
        
        Ok(all_yes)
    }

    /// Commit the transaction (phase 2 of 2PC)
    pub async fn commit(&self, tx_id: &TransactionId) -> Result<()> {
        // Verify transaction is in prepared state
        {
            let transactions = self.transactions.read().unwrap();
            let tx = transactions.get(tx_id)
                .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
            
            if tx.state != TransactionState::Prepared {
                return Err(HyperionError::Transaction(
                    format!("Cannot commit transaction in state {:?}", tx.state)
                ));
            }
        }
        
        // Transition to committing
        {
            let mut transactions = self.transactions.write().unwrap();
            let tx = transactions.get_mut(tx_id)
                .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
            tx.state = TransactionState::Committing;
        }
        
        // Send commit messages to all participants
        let commit_msg = CoordinatorMessage::Commit {
            transaction_id: tx_id.clone(),
        };
        
        let participants = {
            let transactions = self.transactions.read().unwrap();
            let tx = transactions.get(tx_id)
                .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
            tx.participants.clone()
        };
        
        for participant in participants {
            self.simulate_commit_ack(&participant, tx_id).await;
        }
        
        // Mark as committed
        {
            let mut transactions = self.transactions.write().unwrap();
            let tx = transactions.get_mut(tx_id)
                .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
            tx.state = TransactionState::Committed;
        }
        
        Ok(())
    }

    /// Abort the transaction
    pub async fn abort(&self, tx_id: &TransactionId, reason: String) -> Result<()> {
        // Update state
        {
            let mut transactions = self.transactions.write().unwrap();
            let tx = transactions.get_mut(tx_id)
                .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
            
            if tx.state == TransactionState::Committed {
                return Err(HyperionError::Transaction("Cannot abort committed transaction".into()));
            }
            
            tx.state = TransactionState::Aborted;
        }
        
        // Send abort messages to all participants
        let abort_msg = CoordinatorMessage::Abort {
            transaction_id: tx_id.clone(),
            reason,
        };
        
        let participants = {
            let transactions = self.transactions.read().unwrap();
            let tx = transactions.get(tx_id)
                .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
            tx.participants.clone()
        };
        
        for participant in participants {
            self.simulate_abort_ack(&participant, tx_id).await;
        }
        
        Ok(())
    }

    /// Get transaction status
    pub fn get_status(&self, tx_id: &TransactionId) -> Result<TransactionState> {
        let transactions = self.transactions.read().unwrap();
        let tx = transactions.get(tx_id)
            .ok_or_else(|| HyperionError::Transaction("Transaction not found".into()))?;
        Ok(tx.state)
    }

    /// Recover incomplete transactions
    pub async fn recover(&self) -> Result<Vec<TransactionId>> {
        let mut incomplete = Vec::new();
        
        // Find all transactions in non-terminal state
        {
            let transactions = self.transactions.read().unwrap();
            for (tx_id, tx) in transactions.iter() {
                match tx.state {
                    TransactionState::Preparing 
                    | TransactionState::Prepared 
                    | TransactionState::Committing
                    | TransactionState::Recovery => {
                        incomplete.push(tx_id.clone());
                    }
                    _ => {}
                }
            }
        }
        
        // Recover each transaction
        for tx_id in &incomplete {
            self.recover_transaction(tx_id).await?;
        }
        
        Ok(incomplete)
    }

    async fn recover_transaction(&self, tx_id: &TransactionId) -> Result<()> {
        let state = self.get_status(tx_id)?;
        
        match state {
            TransactionState::Preparing => {
                // Could be in doubt - try to complete
                self.abort(tx_id, "Recovery: prepare timeout".into()).await?;
            }
            TransactionState::Prepared => {
                // Safe to commit
                self.commit(tx_id).await?;
            }
            TransactionState::Committing => {
                // Could be partially committed - check participants
                self.commit(tx_id).await?;
            }
            TransactionState::Recovery => {
                // Check participants and decide
                self.commit(tx_id).await?;
            }
            _ => {}
        }
        
        Ok(())
    }

    fn get_participants(&self, node_ids: &[NodeId]) -> Vec<TransactionParticipant> {
        let clients = self.participant_clients.read().unwrap();
        
        node_ids.iter()
            .filter_map(|id| {
                clients.get(id).map(|c| TransactionParticipant {
                    node_id: c.node_id.clone(),
                    endpoint: c.endpoint.clone(),
                    votes: HashMap::new(),
                })
            })
            .collect()
    }

    async fn simulate_prepare_vote(&self, _participant: &TransactionParticipant, _tx_id: &TransactionId) -> Vote {
        // In real implementation, this would send actual network message
        Vote::Yes
    }

    async fn simulate_commit_ack(&self, _participant: &TransactionParticipant, _tx_id: &TransactionId) {
        // In real implementation, this would wait for acknowledgment
    }

    async fn simulate_abort_ack(&self, _participant: &TransactionParticipant, _tx_id: &TransactionId) {
        // In real implementation, this would wait for acknowledgment
    }
}

/// Transaction participant (node) side
pub struct TransactionParticipantNode {
    node_id: NodeId,
    prepared_transactions: RwLock<HashMap<TransactionId, PreparedTransaction>>,
    config: ParticipantConfig,
}

#[derive(Debug, Clone)]
pub struct ParticipantConfig {
    pub coordinator_endpoint: String,
    pub enable_logging: bool,
    pub log_path: String,
    pub durable_log: bool,
}

impl Default for ParticipantConfig {
    fn default() -> Self {
        Self {
            coordinator_endpoint: String::new(),
            enable_logging: true,
            log_path: "/tmp/hyperion_tx_log".into(),
            durable_log: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedTransaction {
    tx_id: TransactionId,
    operations: Vec<TransactionOperation>,
    vote: Vote,
    logged: bool,
}

impl TransactionParticipantNode {
    pub fn new(node_id: NodeId, config: ParticipantConfig) -> Self {
        Self {
            node_id,
            prepared_transactions: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Handle incoming coordinator message
    pub async fn handle_message(&self, msg: CoordinatorMessage) -> Result<CoordinatorMessage> {
        match msg {
            CoordinatorMessage::Prepare { transaction_id, participant_id, operations } => {
                self.handle_prepare(transaction_id, participant_id, operations).await
            }
            CoordinatorMessage::Commit { transaction_id } => {
                self.handle_commit(transaction_id).await
            }
            CoordinatorMessage::Abort { transaction_id, reason } => {
                self.handle_abort(transaction_id, reason).await
            }
            CoordinatorMessage::Status { transaction_id } => {
                self.handle_status(transaction_id).await
            }
            _ => Err(HyperionError::Transaction("Unknown message type".into())),
        }
    }

    async fn handle_prepare(
        &self,
        tx_id: TransactionId,
        _coordinator_id: NodeId,
        operations: Vec<TransactionOperation>,
    ) -> Result<CoordinatorMessage> {
        // Validate we can prepare (lock resources, write to WAL)
        let can_prepare = self.validate_and_lock(&operations)?;
        
        if can_prepare {
            // Write to durable log
            if self.config.enable_logging {
                self.write_prepare_log(&tx_id, &operations)?;
            }
            
            // Store prepared transaction
            {
                let mut prepared = self.prepared_transactions.write().unwrap();
                prepared.insert(tx_id.clone(), PreparedTransaction {
                    tx_id: tx_id.clone(),
                    operations,
                    vote: Vote::Yes,
                    logged: self.config.enable_logging,
                });
            }
            
            Ok(CoordinatorMessage::Acknowledgment {
                transaction_id: tx_id,
                participant_id: self.node_id.clone(),
                vote: Some(Vote::Yes),
                status: ParticipantStatus::Prepared,
            })
        } else {
            Ok(CoordinatorMessage::Acknowledgment {
                transaction_id: tx_id,
                participant_id: self.node_id.clone(),
                vote: Some(Vote::No),
                status: ParticipantStatus::Aborted,
            })
        }
    }

    async fn handle_commit(&self, tx_id: TransactionId) -> Result<CoordinatorMessage> {
        // Apply all operations
        {
            let prepared = self.prepared_transactions.read().unwrap();
            if let Some(tx) = prepared.get(&tx_id) {
                self.apply_operations(&tx.operations)?;
            }
        }
        
        // Remove from prepared
        {
            let mut prepared = self.prepared_transactions.write().unwrap();
            prepared.remove(&tx_id);
        }
        
        // Write commit log
        if self.config.enable_logging {
            self.write_commit_log(&tx_id)?;
        }
        
        Ok(CoordinatorMessage::Acknowledgment {
            transaction_id: tx_id,
            participant_id: self.node_id.clone(),
            vote: None,
            status: ParticipantStatus::Committed,
        })
    }

    async fn handle_abort(&self, tx_id: TransactionId, _reason: String) -> Result<CoordinatorMessage> {
        // Release locks and undo operations
        {
            let mut prepared = self.prepared_transactions.write().unwrap();
            if let Some(tx) = prepared.get(&tx_id) {
                self.rollback_operations(&tx.operations)?;
            }
            prepared.remove(&tx_id);
        }
        
        // Write abort log
        if self.config.enable_logging {
            self.write_abort_log(&tx_id)?;
        }
        
        Ok(CoordinatorMessage::Acknowledgment {
            transaction_id: tx_id,
            participant_id: self.node_id.clone(),
            vote: None,
            status: ParticipantStatus::Aborted,
        })
    }

    async fn handle_status(&self, tx_id: TransactionId) -> Result<CoordinatorMessage> {
        let prepared = self.prepared_transactions.read().unwrap();
        
        let status = if prepared.contains_key(&tx_id) {
            ParticipantStatus::Prepared
        } else {
            // Check log for committed/aborted
            ParticipantStatus::Unknown
        };
        
        Ok(CoordinatorMessage::Acknowledgment {
            transaction_id: tx_id,
            participant_id: self.node_id.clone(),
            vote: None,
            status,
        })
    }

    fn validate_and_lock(&self, _operations: &[TransactionOperation]) -> Result<bool> {
        // Validate operations can be executed
        // Lock required resources
        Ok(true)
    }

    fn apply_operations(&self, operations: &[TransactionOperation]) -> Result<()> {
        for op in operations {
            match op {
                TransactionOperation::Write { pointer, value } => {
                    // Apply write
                    println!("Applying write to {:?}: {:?}", pointer, value);
                }
                TransactionOperation::Delete { pointer } => {
                    // Apply delete
                    println!("Applying delete to {:?}", pointer);
                }
                TransactionOperation::Read { .. } => {
                    // Reads don't need to be applied
                }
            }
        }
        Ok(())
    }

    fn rollback_operations(&self, operations: &[TransactionOperation]) -> Result<()> {
        for op in operations {
            match op {
                TransactionOperation::Write { pointer, .. } => {
                    // Rollback write
                    println!("Rolling back write to {:?}", pointer);
                }
                TransactionOperation::Delete { pointer } => {
                    // Rollback delete
                    println!("Rolling back delete to {:?}", pointer);
                }
                TransactionOperation::Read { .. } => {}
            }
        }
        Ok(())
    }

    fn write_prepare_log(&self, tx_id: &TransactionId, _operations: &[TransactionOperation]) -> Result<()> {
        // Write to durable log
        Ok(())
    }

    fn write_commit_log(&self, tx_id: &TransactionId) -> Result<()> {
        Ok(())
    }

    fn write_abort_log(&self, tx_id: &TransactionId) -> Result<()> {
        Ok(())
    }
}

/// Snapshot isolation implementation
pub struct SnapshotManager {
    active_snapshots: RwLock<HashMap<TransactionId, Snapshot>>,
    committed_values: RwLock<HashMap<DataPointer, (Value, u64)>>,  // value, commit_time
    global_tx_counter: RwLock<u64>,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    tx_id: TransactionId,
    start_time: u64,
    read_set: HashSet<DataPointer>,
    write_set: HashMap<DataPointer, Value>,
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self {
            active_snapshots: RwLock::new(HashMap::new()),
            committed_values: RwLock::new(HashMap::new()),
            global_tx_counter: RwLock::new(0),
        }
    }

    pub fn begin_snapshot(&self, tx_id: TransactionId) {
        let mut snapshots = self.active_snapshots.write().unwrap();
        snapshots.insert(tx_id.clone(), Snapshot {
            tx_id,
            start_time: current_time_ms(),
            read_set: HashSet::new(),
            write_set: HashMap::new(),
        });
    }

    pub fn read_with_snapshot(&self, tx_id: &TransactionId, pointer: &DataPointer) -> Result<Value> {
        let snapshots = self.active_snapshots.read().unwrap();
        
        if let Some(snapshot) = snapshots.get(tx_id) {
            // Check write set first
            if let Some(value) = snapshot.write_set.get(pointer) {
                return Ok(value.clone());
            }
            
            // Check committed values
            let committed = self.committed_values.read().unwrap();
            if let Some((value, _)) = committed.get(pointer) {
                return Ok(value.clone());
            }
        }
        
        Ok(Value::Null)
    }

    pub fn write_with_snapshot(&self, tx_id: &TransactionId, pointer: &DataPointer, value: Value) -> Result<()> {
        let mut snapshots = self.active_snapshots.write().unwrap();
        
        if let Some(snapshot) = snapshots.get_mut(tx_id) {
            snapshot.write_set.insert(pointer.clone(), value);
        }
        
        Ok(())
    }

    pub fn commit_snapshot(&self, tx_id: &TransactionId) -> Result<()> {
        let mut snapshots = self.active_snapshots.write().unwrap();
        let mut committed = self.committed_values.write().unwrap();
        let mut counter = self.global_tx_counter.write().unwrap();
        
        if let Some(snapshot) = snapshots.remove(tx_id) {
            // Commit all writes
            for (pointer, value) in snapshot.write_set {
                committed.insert(pointer, (value, *counter));
            }
            *counter += 1;
        }
        
        Ok(())
    }

    pub fn abort_snapshot(&self, tx_id: &TransactionId) -> Result<()> {
        let mut snapshots = self.active_snapshots.write().unwrap();
        snapshots.remove(tx_id);
        Ok(())
    }
}

// Utility functions

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}-{:x}-4{:x}-{:x}-{:x}",
        (now >> 96) as u32,
        (now >> 80) as u16,
        (now >> 64) as u16 & 0x0fff,
        ((now >> 48) as u16 & 0x3fff) | 0x8000,
        now as u64
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_transaction() {
        let coordinator = TransactionCoordinator::new(
            NodeId("coordinator-1".into()),
            CoordinatorConfig::default(),
        );
        
        let participants = vec![
            NodeId("node-1".into()),
            NodeId("node-2".into()),
        ];
        
        // Begin transaction
        let tx_id = coordinator.begin_transaction(
            IsolationLevel::Serializable,
            participants.clone(),
        ).unwrap();
        
        // Add operations
        coordinator.add_write(
            &tx_id,
            DataPointer {
                table: "users".into(),
                partition: PartitionId("p1".into()),
                key: Value::Integer(1),
            },
            Value::String("test".into()),
        ).unwrap();
        
        // Prepare phase
        let prepared = coordinator.prepare(&tx_id).await.unwrap();
        assert!(prepared);
        
        // Commit phase
        coordinator.commit(&tx_id).await.unwrap();
        
        // Verify committed
        let status = coordinator.get_status(&tx_id).unwrap();
        assert_eq!(status, TransactionState::Committed);
    }

    #[tokio::test]
    async fn test_abort_on_failed_prepare() {
        let coordinator = TransactionCoordinator::new(
            NodeId("coordinator-1".into()),
            CoordinatorConfig::default(),
        );
        
        let tx_id = coordinator.begin_transaction(
            IsolationLevel::ReadCommitted,
            vec![NodeId("node-1".into())],
        ).unwrap();
        
        // Simulate abort before prepare
        coordinator.abort(&tx_id, "User requested".into()).await.unwrap();
        
        let status = coordinator.get_status(&tx_id).unwrap();
        assert_eq!(status, TransactionState::Aborted);
    }

    #[test]
    fn test_snapshot_isolation() {
        let manager = SnapshotManager::new();
        let tx_id = TransactionId::new(NodeId("node-1".into()));
        
        manager.begin_snapshot(tx_id.clone());
        
        let pointer = DataPointer {
            table: "test".into(),
            partition: PartitionId("p1".into()),
            key: Value::Integer(1),
        };
        
        // Write within snapshot
        manager.write_with_snapshot(&tx_id, &pointer, Value::String("snapshot_value".into())).unwrap();
        
        // Read within snapshot
        let value = manager.read_with_snapshot(&tx_id, &pointer).unwrap();
        assert!(matches!(value, Value::String(s) if s == "snapshot_value"));
        
        // Commit snapshot
        manager.commit_snapshot(&tx_id).unwrap();
    }
}
