# Fault-Tolerant Distributed LLM Service

This project demonstrates a robust distributed conversational AI platform. The system is designed to provide seamless availability and fault tolerance across multiple servers, ensuring no interruptions in user experience, even in the face of server crashes or network partitions.

## Features
1. **Multi-Node Consensus**: Implements Multi-Paxos to ensure all nodes agree on updates to the system.  
2. **Contextual Query Handling**: Users can create, query, and manage conversation contexts stored in a replicated key-value store.  
3. **Fault Tolerance**: Resilient to server crashes and network partitioning, with automatic recovery and synchronization upon reconnection.  
4. **User-Friendly Interaction**: Offers functionality for users to choose the best AI-generated responses and save them for continuity.  
5. **LLM Integration**: Utilizes Gemini LLM for generating conversational AI responses via an API.  

---

## Getting Started

### Prerequisites
- **Programming Languages**: Python  
- **Dependencies**: Install required packages using:  
  ```bash
  pip install -r requirements.txt
