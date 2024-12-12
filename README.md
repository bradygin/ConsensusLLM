# ConsensusLLM

A distributed, fault-tolerant system that integrates Large Language Models using Multi-Paxos consensus protocol. ConsensusLLM ensures uninterrupted AI services by maintaining consistency across multiple nodes while gracefully handling server failures and network partitions.

## Overview

ConsensusLLM uses distributed systems principles to provide robust, fault-tolerant conversational AI services. Built on Google's Gemini LLM, the system ensures continuity of service even when individual nodes fail or network issues occur.

### Core Components
1. **Network Orchestrator**: Manages inter-node communication and simulates network conditions
2. **Consensus Nodes**: Distributed servers implementing Multi-Paxos for state replication
3. **Replicated Store**: Distributed key-value store maintaining conversation contexts

### Key Features
- **High Availability**: Continues operation despite node failures or network partitions
- **Strong Consistency**: Ensures synchronized state across all active nodes
- **Dynamic Leadership**: Automatic leader election and failover
- **Self Recovery**: Nodes automatically recover and resynchronize after failures
- **Gemini Integration**: Leverages Google's Gemini API for AI capabilities

## Technical Architecture

### Consensus Implementation
ConsensusLLM implements Multi-Paxos with the following characteristics:
- **Ballot Structure**: 3-tuple format `<seq_num, pid, op_num>` for precise operation tracking
- **Two-Phase Consensus**: Implements Prepare/Promise and Accept/Accepted phases
- **Operation Ordering**: Maintains sequential consistency through operation queuing
- **Automatic Failover**: Leader election triggers on failures

### State Management
- Synchronized key-value store for conversation state
- Guaranteed operation ordering across distributed nodes
- Automatic state recovery for rejoining nodes

## Setup

### Prerequisites
- Python 3.8+
- tmux
- Google Cloud Console Account (for Gemini API access)

### Installation
```bash
# Clone the repository
git clone https://github.com/bradygin/ConsensusLLM.git
cd ConsensusLLM

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure Gemini API
cp config.json
# Add your Gemini API key to a config.json file
```

### Deployment
```bash
# Launch distributed system
./run.sh
```

## Usage

### Basic Operations
```bash
# Initialize conversation context
create c1

# Send query to LLM
query c1 What are the implications of distributed consensus?

# Select from response candidates
choose c1 2

# View conversation history
view c1

# Display all conversations
viewall
```

### System Testing
```bash
# Simulate node failure
failNode 3

# Create network partition
failLink 1 2

# Restore network connectivity
fixLink 1 2
```
