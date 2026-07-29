### Basic Distributed Approach
1. Make brokers discover each other through HTTP or gRPC.
2. Decide 1 leader randomly (no raft for now).
3. Create metadata service and store data in an internal topic.
4. For starting just store current leader host and port through metadata service.
5. Client can connect to any broker and if it is not leader it can ask metadata service for loader information.
6. Leader will accept write and forward write to followers.


---
This is starting point of our distributed architecture. It will be improved incremently.