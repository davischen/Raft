# Assignment 1 Testing Guide

## Overview

This test suite validates your Assignment 1 implementation for the distributed Raft project. The testing script is **language-agnostic** and will work with implementations in Python, Go, C++, Java, or any other language, provided you follow the specified interface requirements.

## What This Tests

The test suite validates the infrastructure and frontend service requirements for Assignment 1:

- **Frontend Service Setup** (2.5 pts) - Service runs on port 8001
- **Configuration Management** (0.5 pts) - Proper `config.ini` structure  
- **Process Lifecycle** (3.0 pts) - StartRaft creates servers with correct names
- **Server Connectivity** (2.5 pts) - Servers respond to basic RPCs
- **Cluster Management** (2.0 pts) - Different cluster sizes work
- **Individual Server Control** (2.0 pts) - StartServer restarts single servers
- **API Stubs** (2.5 pts) - Get/Put return "Not implemented" errors

**Total: 15 points**

## Requirements for Compatibility

### 1. gRPC Protocol Implementation
Your implementation must use the same `raft.proto` definitions. The key services are:

```protobuf
service FrontEnd {
    rpc StartRaft(IntegerArg) returns (Reply);
    rpc StartServer(IntegerArg) returns (Reply);
    rpc Get(GetKey) returns (Reply);
    rpc Put(KeyValue) returns (Reply);
}

service KeyValueStore {
    rpc ping(Empty) returns (GenericResponse);
    rpc GetState(Empty) returns (State);
    // ... other methods
}
```

### 2. Port Binding Requirements
- **Frontend service**: Must bind to `localhost:8001`
- **Server processes**: Must bind to `localhost:9001+server_id`
  - Server 0 → `localhost:9001`
  - Server 1 → `localhost:9002`
  - Server 2 → `localhost:9003`
  - etc.

### 3. Process Naming Convention
Your server processes must be identifiable by one of these patterns:
- **Preferred**: `raftserver{N}` where N = server_id + 1
  - Server 0 → process name contains "raftserver1"
  - Server 1 → process name contains "raftserver2"
  - etc.
- **Alternative**: `server.py` (for Python implementations)

The test script uses `ps aux | grep` to find processes, so the pattern must appear in the process command line.

## File Structure

```
assignment1/
├── assignment1_test.py          # Test script (provided)
├── setup.sh                    # Setup helper (provided)
├── raft.proto                  # Protocol definition (provided)
├── config.ini                  # Your configuration file
├── frontend.py|go|cpp          # Your frontend implementation
├── server.py|go|cpp            # Your server implementation
└── README.md                   # Your documentation
```

## Setup Instructions

### Step 1: Install Dependencies
```bash
# For Python testing environment
pip install grpcio grpcio-tools

# Generate protobuf files (if using Python)
python -m grpc_tools.protoc --python_out=. --grpc_python_out=. raft.proto
```

### Step 2: Verify Configuration
Ensure your `config.ini` contains the required sections:

```ini
[Global]
base_address = 127.0.0.1

[Servers]
base_port = 9001
base_source_port = 7001
max_workers = 10
persistent_state_path = memory
active = 0,1,2,3,4
```

### Step 3: Implement Required Methods

#### Frontend Service (`localhost:8001`)
```
StartRaft(n) → Start n servers with correct names, clean state
StartServer(id) → Start individual server without cleaning state  
Get(key) → Return "Not implemented" error
Put(key, value) → Return "Not implemented" error
```

#### Server Stubs (`localhost:9001+`)
```
ping() → Return success=True
GetState() → Return term=0, isLeader=False
```

## Running Tests

### Method 1: Automated Setup
```bash
# Run setup helper
chmod +x setup.sh
./setup.sh

# Start your frontend service
python frontend.py &  # or ./frontend_binary

# Run tests
python assignment1_test.py
```

### Method 2: Manual Setup
```bash
# Start your frontend service in one terminal
python frontend.py
# OR
./your_frontend_binary

# Run tests in another terminal
python assignment1_test.py
```

## Language-Specific Examples

### Python Implementation
```python
# Process creation in frontend.py
def start_server(self, server_id):
    cmd = ["python", "server.py", str(server_id)]
    # Use exec to set process name for killability
    process = subprocess.Popen(
        ["bash", "-c", f"exec -a raftserver{server_id+1} python server.py {server_id}"]
    )
```

### Go Implementation
```go
// In your frontend service
func (s *frontendServer) StartRaft(ctx context.Context, req *pb.IntegerArg) (*pb.Reply, error) {
    n := int(req.Arg)
    for i := 0; i < n; i++ {
        // Start server with correct process name
        cmd := exec.Command("./server", strconv.Itoa(i))
        cmd.Args[0] = fmt.Sprintf("raftserver%d", i+1)
        cmd.Start()
    }
    return &pb.Reply{}, nil
}
```

### C++ Implementation
```cpp
// In your frontend service
grpc::Status StartRaft(grpc::ServerContext* context, 
                      const IntegerArg* request, 
                      Reply* response) override {
    int n = request->arg();
    for (int i = 0; i < n; i++) {
        std::string cmd = "./server " + std::to_string(i);
        std::string name = "raftserver" + std::to_string(i + 1);
        // Set process name and start
        system(cmd.c_str());
    }
    return grpc::Status::OK;
}
```

## Common Issues and Solutions

### Issue: "Frontend not running on port 8001"
**Solution**: Ensure your frontend service is started and listening on the correct port
```bash
# Check if port is in use
netstat -tulpn | grep 8001
# Or
lsof -i :8001
```

### Issue: "Server processes not found"
**Solution**: Verify process naming convention
```bash
# Check your process names
ps aux | grep raftserver
# Should show: raftserver1, raftserver2, etc.
```

### Issue: "RPC failed" errors
**Solutions**:
- Verify protobuf files are generated correctly
- Check server ports are available and not blocked by firewall
- Ensure servers implement the required RPC methods

### Issue: "Wrong process names found"
**Solution**: Make sure your StartRaft implementation sets process names correctly:
- Server 0 should have process name containing "raftserver1"
- Server 1 should have process name containing "raftserver2"
- etc.

## Understanding Test Output

```
===== ASSIGNMENT 1 TEST RESULTS (2024-03-15 14:30:22) =====
Config File                    [PASS] 0.5/0.5 — Configuration file has all required sections/keys
Frontend Service               [PASS] 2.5/2.5 — Frontend service responding on port 8001
StartRaft Basic                [PASS] 3.0/3.0 — Successfully started 3 servers with correct names
Server Connectivity            [PASS] 2.5/2.5 — All servers responding to ping and GetState correctly
StartRaft Sizes                [PASS] 2.0/2.0 — StartRaft works with different cluster sizes
Start Server                   [PASS] 2.0/2.0 — Individual server restart successful
Unimplemented Ops              [PASS] 2.5/2.5 — Get/Put correctly return 'Not implemented'

Tests run: 7, Passed: 7, Failed: 0 (100.0% pass rate)
Overall Score: 15.0/15
==================================================
```

### Status Meanings:
- **PASS**: Full points earned
- **PART**: Partial credit
- **FAIL**: No points earned

## Tips for Success

1. **Start Simple**: Implement basic connectivity first, then add process management
2. **Test Incrementally**: Run tests after each feature implementation
3. **Check Logs**: Most issues are revealed in server/frontend logs
4. **Process Management**: Ensure proper cleanup of child processes
5. **Port Conflicts**: Make sure no other services are using required ports

## Getting Help

If tests are failing:

1. **Check Prerequisites**: Verify all required files exist and ports are available
2. **Review Logs**: Look at server output for error messages
3. **Manual Testing**: Try RPC calls manually using tools like `grpcurl`
4. **Process Inspection**: Use `ps aux | grep raftserver` to verify process creation
