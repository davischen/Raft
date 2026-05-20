#!/usr/bin/env python3
"""
Assignment 1 Test Script: Infrastructure and Frontend Service
Tests basic gRPC infrastructure, process management, and configuration.
"""

import subprocess
import time
import grpc
import sys
import os
from datetime import datetime
import configparser

# Import the generated protobuf files (assuming they exist)
try:
    import raft_pb2
    import raft_pb2_grpc
except ImportError:
    print("ERROR: Could not import raft_pb2 or raft_pb2_grpc")
    print("Please generate them from raft.proto first:")
    print("  python -m grpc_tools.protoc --python_out=. --grpc_python_out=. raft.proto")
    sys.exit(1)

class TestResult:
    def __init__(self, name, score, max_points, details):
        self.name = name
        self.score = score
        self.max_points = max_points
        self.details = details

class TestSuite:
    def __init__(self):
        self.results = []
        self.total = 0

    def add(self, result):
        self.results.append(result)
        self.total += result.score

    def print_results(self):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n===== ASSIGNMENT 1 TEST RESULTS ({now}) =====")
        
        passed = 0
        for r in self.results:
            if r.score == r.max_points:
                status = "PASS"
                passed += 1
            elif r.score > 0:
                status = "PART"
            else:
                status = "FAIL"
            
            print(f"{r.name:<30} [{status}] {r.score:.1f}/{r.max_points:.1f} — {r.details}")
        
        total_tests = len(self.results)
        failed = total_tests - passed
        pass_rate = (passed / total_tests * 100) if total_tests > 0 else 0
        
        print(f"\nTests run: {total_tests}, Passed: {passed}, Failed: {failed} ({pass_rate:.1f}% pass rate)")
        print(f"Overall Score: {self.total:.1f}/15")
        print("=" * 50)

# Constants
FRONTEND_ADDR = "localhost:8001"
BASE_PORT = 9001
NUM_SERVERS = 5
RPC_TIMEOUT = 5

def cleanup_processes():
    """Kill any existing raft server processes"""
    print("Cleaning up existing processes...")
    
    # Method 1: Kill by process name
    for i in range(1, 6):  # raftserver1 through raftserver5
        try:
            result = subprocess.run(["pkill", "-f", f"raftserver{i}"], 
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=3)
            if result.returncode == 0:
                print(f"  Killed raftserver{i}")
        except:
            pass
    
    # Method 2: Kill server.py processes
    try:
        result = subprocess.run(["pgrep", "-f", "server.py"], 
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=3)
        if result.stdout.strip():
            pids = result.stdout.strip().split('\n')
            print(f"  Found server.py PIDs to kill: {pids}")
            for pid in pids:
                try:
                    subprocess.run(["kill", pid], timeout=2)
                    print(f"  Killed PID {pid}")
                except:
                    try:
                        subprocess.run(["kill", "-9", pid], timeout=2)
                        print(f"  Force killed PID {pid}")
                    except:
                        pass
    except:
        pass
    
    # Method 3: Generic pkill for server.py
    try:
        subprocess.run(["pkill", "-f", "server.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=3)
    except:
        pass
    
    # Give processes time to die
    time.sleep(3)
    
    # Verify cleanup
    try:
        result = subprocess.run(["pgrep", "-f", "server.py"], 
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=3)
        if result.stdout.strip():
            remaining_pids = result.stdout.strip().split('\n')
            print(f"  Warning: Some server processes may still be running: {remaining_pids}")
        else:
            print("  All server processes cleaned up successfully")
    except:
        pass

def check_frontend_running():
    """Check if frontend service is running on port 8001"""
    try:
        channel = grpc.insecure_channel(FRONTEND_ADDR)
        stub = raft_pb2_grpc.FrontEndStub(channel)
        
        # Try a simple call to verify it's responding
        request = raft_pb2.GetKey(key="test", clientId=1, requestId=1)
        response = stub.Get(request, timeout=3)
        channel.close()
        
        # Should get "Not implemented" error, which means it's working
        return response.wrongLeader and "Not implemented" in response.error
    except Exception as e:
        print(f"Frontend check failed: {e}")
        return False

def call_start_raft(n):
    """Call StartRaft RPC with n servers"""
    try:
        channel = grpc.insecure_channel(FRONTEND_ADDR)
        stub = raft_pb2_grpc.FrontEndStub(channel)
        
        request = raft_pb2.IntegerArg(arg=n)
        response = stub.StartRaft(request, timeout=15)  # Increased timeout
        channel.close()
        
        if response.error:
            return False, response.error
        return True, ""
    except Exception as e:
        return False, str(e)

def call_start_server(server_id):
    """Call StartServer RPC for specific server"""
    try:
        channel = grpc.insecure_channel(FRONTEND_ADDR)
        stub = raft_pb2_grpc.FrontEndStub(channel)
        
        request = raft_pb2.IntegerArg(arg=server_id)
        response = stub.StartServer(request, timeout=10)  # Increased timeout
        channel.close()
        
        if response.error:
            return False, response.error
        return True, ""
    except Exception as e:
        return False, str(e)

def ping_server(server_id):
    """Ping a specific server"""
    try:
        addr = f"localhost:{BASE_PORT + server_id}"
        channel = grpc.insecure_channel(addr)
        stub = raft_pb2_grpc.KeyValueStoreStub(channel)
        
        request = raft_pb2.Empty()
        response = stub.ping(request, timeout=3)
        channel.close()
        
        return response.success
    except:
        return False

def get_server_state(server_id):
    """Get state from a specific server"""
    try:
        addr = f"localhost:{BASE_PORT + server_id}"
        channel = grpc.insecure_channel(addr)
        stub = raft_pb2_grpc.KeyValueStoreStub(channel)
        
        request = raft_pb2.Empty()
        response = stub.GetState(request, timeout=3)
        channel.close()
        
        return True, response.term, response.isLeader
    except Exception as e:
        return False, 0, False

def check_process_names():
    """Check if server processes have correct names"""
    try:
        # Check using ps aux (compatible with older Python)
        result = subprocess.run(["ps", "aux"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes = result.stdout
        
        found_servers = []
        
        print("Debug: Looking for process names...")
        print("All processes containing 'raftserver' or 'server.py':")
        
        for line in processes.split('\n'):
            if 'raftserver' in line or 'server.py' in line:
                print(f"  {line}")
                
                # Check for raftserver names
                for i in range(1, 6):
                    if f"raftserver{i}" in line:
                        found_servers.append(i)
        
        # Also try alternative method - check for python server.py processes
        alt_servers = []
        try:
            result2 = subprocess.run(["pgrep", "-f", "server.py"], 
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result2.stdout.strip():
                pids = result2.stdout.strip().split('\n')
                print(f"Found {len(pids)} server.py processes with PIDs: {pids}")
                
                # For each PID, try to determine which server it is
                for pid in pids:
                    try:
                        cmdline_result = subprocess.run(
                            ["ps", "-p", pid, "-o", "args="], 
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                        )
                        if cmdline_result.returncode == 0:
                            cmdline = cmdline_result.stdout.strip()
                            print(f"  PID {pid}: {cmdline}")
                            
                            # Extract server ID from command line
                            if "server.py" in cmdline:
                                parts = cmdline.split()
                                if len(parts) >= 3 and parts[-1].isdigit():
                                    server_id = int(parts[-1])
                                    if 0 <= server_id <= 4:
                                        alt_servers.append(server_id + 1)  # Convert to raftserver number
                    except:
                        pass
        except:
            pass
        
        print(f"Found raftserver names: {found_servers}")
        print(f"Found server.py processes: {alt_servers}")
        
        # Return the union of both methods
        all_found = list(set(found_servers + alt_servers))
        return all_found
        
    except Exception as e:
        print(f"Error checking process names: {e}")
        return []

def test_frontend_service():
    """Test 1: Frontend Service Startup"""
    print("\n=== Test: Frontend Service Startup ===")
    
    if not check_frontend_running():
        return TestResult("Frontend Service", 0, 2.5, 
                         "Frontend not running on port 8001")
    
    return TestResult("Frontend Service", 2.5, 2.5, 
                     "Frontend service responding on port 8001")

def test_start_raft_basic():
    """Test 2: Basic StartRaft Functionality"""
    print("\n=== Test: Basic StartRaft Functionality ===")
    
    cleanup_processes()
    
    # Test starting 3 servers
    print("Calling StartRaft(3)...")
    success, error = call_start_raft(3)
    if not success:
        return TestResult("StartRaft Basic", 0, 3.0, 
                         f"StartRaft RPC failed: {error}")
    
    print("StartRaft succeeded, waiting for servers...")
    # Wait for servers to start
    time.sleep(5)  # Increased wait time
    
    # Check if servers are responding (most important test)
    responding_servers = []
    for i in range(3):
        if ping_server(i):
            responding_servers.append(i)
    
    print(f"Servers responding to ping: {responding_servers}")
    
    # Check if processes are running with correct names
    found_servers = check_process_names()
    expected_servers = [1, 2, 3]  # raftserver1, raftserver2, raftserver3
    
    print(f"Expected servers: {expected_servers}, Found: {found_servers}")
    
    # Scoring logic:
    # 3.0 points if all servers responding AND have correct names
    # 2.5 points if all servers responding but wrong/missing names  
    # 1.5 points if some servers responding
    # 0 points if no servers responding
    
    if len(responding_servers) == 3:
        if all(s in found_servers for s in expected_servers):
            return TestResult("StartRaft Basic", 3.0, 3.0,
                             "Successfully started 3 servers with correct names")
        else:
            return TestResult("StartRaft Basic", 2.5, 3.0,
                             f"3 servers responding but process names issue (expected {expected_servers}, found {found_servers})")
    elif len(responding_servers) > 0:
        return TestResult("StartRaft Basic", 1.5, 3.0,
                         f"Only {len(responding_servers)}/3 servers responding")
    else:
        return TestResult("StartRaft Basic", 0, 3.0,
                         "No servers responding after StartRaft")

def test_server_connectivity():
    """Test 3: Server RPC Connectivity"""
    print("\n=== Test: Server RPC Connectivity ===")
    
    # Test ping functionality
    ping_results = []
    print("Testing server pings...")
    for i in range(3):  # Test servers 0, 1, 2
        if ping_server(i):
            ping_results.append(i)
            print(f"  Server {i}: ping OK")
        else:
            print(f"  Server {i}: ping FAILED")
    
    if len(ping_results) != 3:
        return TestResult("Server Connectivity", 1.0, 2.5,
                         f"Ping failed, only {len(ping_results)}/3 servers responding")
    
    # Test GetState functionality
    state_results = []
    print("Testing server GetState...")
    for i in range(3):
        success, term, is_leader = get_server_state(i)
        if success and term == 0 and not is_leader:
            state_results.append(i)
            print(f"  Server {i}: GetState OK (term={term}, leader={is_leader})")
        else:
            print(f"  Server {i}: GetState FAILED or wrong values (success={success}, term={term}, leader={is_leader})")
    
    if len(state_results) != 3:
        return TestResult("Server Connectivity", 1.5, 2.5,
                         f"GetState failed or wrong values, {len(state_results)}/3 correct")
    
    return TestResult("Server Connectivity", 2.5, 2.5,
                     "All servers responding to ping and GetState correctly")

def test_start_raft_different_sizes():
    """Test 4: StartRaft with Different Cluster Sizes"""
    print("\n=== Test: StartRaft with Different Sizes ===")
    
    # Test starting 5 servers
    cleanup_processes()
    print("Calling StartRaft(5)...")
    success, error = call_start_raft(5)
    if not success:
        return TestResult("StartRaft Sizes", 0, 2.0,
                         f"StartRaft(5) failed: {error}")
    
    time.sleep(5)
    
    # Check if servers are responding
    responding_servers = []
    for i in range(5):
        if ping_server(i):
            responding_servers.append(i)
    
    print(f"Servers responding to ping: {responding_servers}")
    
    found_servers = check_process_names()
    expected_servers = [1, 2, 3, 4, 5]
    
    print(f"Expected process names: {expected_servers}, Found: {found_servers}")
    
    # Scoring:
    # 2.0 points if all 5 servers responding AND have correct names
    # 1.5 points if all 5 servers responding but wrong/missing names
    # 1.0 points if some servers responding
    # 0 points if no servers responding
    
    if len(responding_servers) == 5:
        if all(s in found_servers for s in expected_servers):
            return TestResult("StartRaft Sizes", 2.0, 2.0,
                             "StartRaft(5) works with correct process names")
        else:
            return TestResult("StartRaft Sizes", 1.5, 2.0,
                             f"StartRaft(5) - 5 servers responding but process names issue")
    elif len(responding_servers) > 0:
        return TestResult("StartRaft Sizes", 1.0, 2.0,
                         f"StartRaft(5) - Only {len(responding_servers)}/5 servers responding")
    else:
        return TestResult("StartRaft Sizes", 0, 2.0,
                         "StartRaft(5) - No servers responding")

def test_start_server_individual():
    """Test 5: Individual Server Start/Restart"""
    print("\n=== Test: Individual Server Start/Restart ===")
    
    # First, ensure we have servers running
    print("Ensuring we have servers running...")
    if not ping_server(0) or not ping_server(1) or not ping_server(2):
        print("Not all servers responding, starting fresh cluster...")
        cleanup_processes()
        success, error = call_start_raft(3)
        if not success:
            return TestResult("Start Server", 0, 2.0, f"Could not start fresh cluster: {error}")
        time.sleep(5)
    
    # Find and kill server 2 specifically
    print("Finding and killing server 2...")
    server_2_killed = False
    
    try:
        # Find server.py processes and their command lines
        result = subprocess.run(["pgrep", "-f", "server.py"], 
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.stdout.strip():
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                try:
                    # Get command line for this PID
                    cmdline_result = subprocess.run(
                        ["ps", "-p", pid, "-o", "args="], 
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                    )
                    if cmdline_result.returncode == 0:
                        cmdline = cmdline_result.stdout.strip()
                        # Check if this is server 2 (server.py 2)
                        if "server.py 2" in cmdline:
                            print(f"Found server 2 with PID {pid}: {cmdline}")
                            subprocess.run(["kill", pid], timeout=2)
                            time.sleep(2)
                            server_2_killed = True
                            break
                except:
                    continue
    except:
        pass
    
    # Fallback: try killing by name
    if not server_2_killed:
        try:
            subprocess.run(["pkill", "-f", "raftserver3"], timeout=3)
            subprocess.run(["pkill", "-f", "server.py 2"], timeout=3)
            time.sleep(2)
            server_2_killed = True
        except:
            pass
    
    # Verify server 2 is down
    max_attempts = 5
    server_down = False
    for attempt in range(max_attempts):
        if not ping_server(2):
            server_down = True
            print(f"Server 2 is down (attempt {attempt + 1})")
            break
        print(f"Server 2 still responding, attempt {attempt + 1}/{max_attempts}")
        time.sleep(1)
    
    if not server_down:
        return TestResult("Start Server", 0, 2.0, 
                         "Could not kill server 2 for testing")
    
    # Restart it using StartServer
    print("Restarting server 2 using StartServer RPC...")
    success, error = call_start_server(2)
    if not success:
        return TestResult("Start Server", 1.0, 2.0,
                         f"StartServer(2) failed: {error}")
    
    time.sleep(3)
    
    # Verify it's back up
    if not ping_server(2):
        return TestResult("Start Server", 1.5, 2.0,
                         "StartServer succeeded but server not responding")
    
    print("Server 2 successfully restarted!")
    return TestResult("Start Server", 2.0, 2.0,
                     "Individual server restart successful")

def test_unimplemented_operations():
    """Test 6: Get/Put Return Not Implemented"""
    print("\n=== Test: Unimplemented Operations ===")
    
    try:
        channel = grpc.insecure_channel(FRONTEND_ADDR)
        stub = raft_pb2_grpc.FrontEndStub(channel)
        
        # Test Get operation
        get_request = raft_pb2.GetKey(key="test", clientId=1, requestId=1)
        get_response = stub.Get(get_request, timeout=3)
        
        # Test Put operation  
        put_request = raft_pb2.KeyValue(key="test", value="val", clientId=1, requestId=2)
        put_response = stub.Put(put_request, timeout=3)
        
        channel.close()
        
        # Both should return wrongLeader=True with "Not implemented" error
        get_ok = get_response.wrongLeader and "Not implemented" in get_response.error
        put_ok = put_response.wrongLeader and "Not implemented" in put_response.error
        
        if get_ok and put_ok:
            return TestResult("Unimplemented Ops", 2.5, 2.5,
                             "Get/Put correctly return 'Not implemented'")
        elif get_ok or put_ok:
            return TestResult("Unimplemented Ops", 1.5, 2.5,
                             "Only one of Get/Put returns correct error")
        else:
            return TestResult("Unimplemented Ops", 0, 2.5,
                             "Get/Put don't return expected 'Not implemented' errors")
            
    except Exception as e:
        return TestResult("Unimplemented Ops", 0, 2.5,
                         f"RPC failed: {e}")

def test_config_file():
    """Test 7: Configuration File"""
    print("\n=== Test: Configuration File ===")
    
    if not os.path.exists("config.ini"):
        return TestResult("Config File", 0, 0.5,
                         "config.ini file not found")
    
    try:
        config = configparser.ConfigParser()
        config.read("config.ini")
        
        # Check required sections and keys
        required_checks = [
            ("Global", "base_address"),
            ("Servers", "base_port"),
            ("Servers", "base_source_port"),
            ("Servers", "max_workers"),
            ("Servers", "persistent_state_path"),
            ("Servers", "active")
        ]
        
        for section, key in required_checks:
            if not config.has_option(section, key):
                return TestResult("Config File", 0.25, 0.5,
                                 f"Missing {section}.{key}")
        
        return TestResult("Config File", 0.5, 0.5,
                         "Configuration file has all required sections/keys")
        
    except Exception as e:
        return TestResult("Config File", 0, 0.5,
                         f"Error reading config: {e}")

def main():
    print("Assignment 1 Test Suite - Infrastructure and Frontend Service")
    print("=" * 60)
    
    # Check prerequisites
    if not os.path.exists("frontend.py"):
        print("ERROR: frontend.py not found")
        return
    
    if not os.path.exists("server.py"):
        print("ERROR: server.py not found")
        return
    
    print("Please ensure frontend.py is running before starting tests.")
    print("Run: python frontend.py")
    input("Press Enter when frontend is ready...")
    
    # Initialize test suite
    suite = TestSuite()
    
    # Run tests
    suite.add(test_config_file())
    suite.add(test_frontend_service())
    suite.add(test_start_raft_basic())
    suite.add(test_server_connectivity())
    suite.add(test_start_raft_different_sizes())
    suite.add(test_start_server_individual())
    suite.add(test_unimplemented_operations())
    
    # Cleanup
    cleanup_processes()
    
    # Print results
    suite.print_results()

if __name__ == "__main__":
    main()