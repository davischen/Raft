from concurrent import futures

import grpc
import raft_pb2_grpc
import raft_pb2
import subprocess
import logging
import configparser
import os
import sys
import time
import shutil

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

def _kill_all():
    subprocess.run(["pkill", "-f", "server.py"], check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def _clear_persistent_state_if_needed():
    p = config.get("Servers", "persistent_state_path", fallback="memory").strip().lower()
    if p and p != "memory" and os.path.exists(p):
        shutil.rmtree(p, ignore_errors=True)

def _create_peristent_state_if_needed():
    p = config.get("Servers", "persistent_state_path", fallback="memory").strip().lower()
    if p and p != "memory":
        os.makedirs(p, exist_ok=True)

def _wait_ping(addr: str, port: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    target = f"{addr}:{port}"
    while time.time() < deadline:
        try:
            with grpc.insecure_channel(target) as ch:
                raft_pb2_grpc.KeyValueStoreStub(ch).ping(raft_pb2.Empty(), timeout=1.0)
                return True
        except Exception:
            time.sleep(0.15)
    return False

class FrontEnd(raft_pb2_grpc.FrontEndServicer):
    def StartRaft(self, request, context):
        _kill_all()
        _clear_persistent_state_if_needed()
        _create_peristent_state_if_needed()
        for server_id in range(request.arg):
            cmd = ["python", "server.py", str(server_id)]
            logf = open(os.path.join(os.path.dirname(CONFIG_PATH), f"server_{server_id}.log"), "ab", buffering=0)

            # This command fails with GradeScope but works locally
            #cmd = ["bash", "-c", f"exec -a raftserver{server_id+1} python server.py {server_id}"],
            process = subprocess.Popen(
                            cmd,
                            stdout=logf, stderr=logf,
                            cwd=os.path.dirname(CONFIG_PATH),
                            start_new_session=True
            )
            try: logf.close()
            except: pass

        config["Servers"]["active"] = ",".join([str(i) for i in range(request.arg)])
        with open(CONFIG_PATH, 'w+') as f:
            config.write(f)
 
        # health check
        base_addr = config.get("Global", "base_address", fallback="127.0.0.1")
        base_port = config.getint("Servers", "base_port", fallback=9001)
        for server_id in range(request.arg):
            ok = _wait_ping(base_addr, base_port + server_id, timeout=2.0)
            if not ok:
                _kill_all()
                return raft_pb2.Reply(
                    wrongLeader=True,
                    error=f"Server {server_id} failed health check on {base_addr}:{base_port+server_id}"
                )
        
       
        return raft_pb2.Reply(wrongLeader=False)

    def StartServer(self, request, context):
        server_id = request.arg
        cmd = ["python", "server.py", str(server_id)]
        logf = open(os.path.join(os.path.dirname(CONFIG_PATH), f"server_{server_id}.log"), "ab", buffering=0)
        
        process = subprocess.Popen(
                            cmd,
                            stdout=logf, stderr=logf,
                            cwd=os.path.dirname(CONFIG_PATH),
                            start_new_session=True
        )
        try: logf.close()
        except: pass

        # health check
        base_addr = config.get("Global", "base_address", fallback="127.0.0.1")
        base_port = config.getint("Servers", "base_port", fallback=9001)
        if not _wait_ping(base_addr, base_port + server_id, timeout=2.0):
            return raft_pb2.Reply(wrongLeader=True, error=f"Server {server_id} failed health check")

        return raft_pb2.Reply(wrongLeader=False)

    def find_leader_server(self):
        active_servers = self.get_active_servers_from_config()
        for server_id in active_servers:
            try:
                success, term, is_leader = self.get_server_state(server_id)
                if success and is_leader:
                    return server_id
            except: 
                continue
        return self.find_available_server()

    def get_server_state(self, server_id):
        deadline = time.time() + 1.0

        # todo refactor
        base_addr = config.get("Global", "base_address", fallback="127.0.0.1")
        base_port = config.getint("Servers", "base_port", fallback=9001)
        target_port = str(base_port + server_id)
        target = f"{base_addr}:{target_port}"

        while time.time() < deadline:
            try:
                with grpc.insecure_channel(target) as ch:
                    stub = raft_pb2_grpc.KeyValueStoreStub(ch)
                    response = stub.GetState(raft_pb2.Empty())
                    return True, response.term, response.isLeader

            except Exception:
                time.sleep(0.15)
        return False, None, None

    def find_available_server(self):
        active_servers = self.get_active_servers_from_config()
        for server_id in active_servers:
            server_id = int(server_id)
            if self.ping_server(server_id):
                logging.info(f"ping returned ok for {server_id}")
                return server_id
            else:
                logging.info("ping failed")
        return None

    def ping_server(self, server_id):
        base_addr = config.get("Global", "base_address", fallback="127.0.0.1")
        base_port = config.getint("Servers", "base_port", fallback=9001)
        logging.info(f"Going to ping {base_addr}:{base_port}")

        return _wait_ping(base_addr, base_port + server_id, timeout=1.0)

    def Get(self, request, context):
        server_id = self.find_leader_server()
        if server_id is None:
            return raft_pb2.Reply(wrongLeader=True, error="No servers available")

        success, value = self.forward_get_to_server(server_id, request.key)
        if success:
            return raft_pb2.Reply(wrongLeader=False, value=value)

        return raft_pb2.Reply(wrongLeader=True, error="Server error")

    def forward_get_to_server(self, server_id, key):
        deadline = time.time() + 1.0
        base_addr = config.get("Global", "base_address", fallback="127.0.0.1")
        base_port = config.getint("Servers", "base_port", fallback=9001)
        target_port = str(base_port + server_id)
        target = f"{base_addr}:{target_port}"
        
        while time.time() < deadline:
            try:
                with grpc.insecure_channel(target) as ch:
                    stub = raft_pb2_grpc.KeyValueStoreStub(ch)
                    request = raft_pb2.StringArg(arg=key)
                    response = stub.Get(request)
                    return True, response.value

            except Exception:
                time.sleep(0.15)
        return False, ""

    def Put(self, request, context):
        server_id = self.find_leader_server()
        if server_id is None:
            return raft_pb2.Reply(wrongLeader=True, error="No servers available")

        success = self.forward_put_to_server(server_id, request.key, request.value)
        if success:
            return raft_pb2.Reply(wrongLeader=False)

        return raft_pb2.Reply(wrongLeader=True, error="unable to put key to server")

    def forward_put_to_server(self, server_id, key, value):
        deadline = time.time() + 1.0
        base_addr = config.get("Global", "base_address", fallback="127.0.0.1")
        base_port = config.getint("Servers", "base_port", fallback=9001)
        target_port = str(base_port + server_id)
        target = f"{base_addr}:{target_port}"

        while time.time() < deadline:
            try:
                with grpc.insecure_channel(target) as ch:
                    stub = raft_pb2_grpc.KeyValueStoreStub(ch)
                    request = raft_pb2.KeyValue(key=key, value=value)
                    response = stub.Put(request)
                    return True

            except Exception:
                time.sleep(0.15)
        return False

    def get_active_servers_from_config(self):
        return config["Servers"].get("active").split(",")

if __name__ == "__main__":
    logging.basicConfig(filename='frontend.log', level=logging.INFO)
    #logging.basicConfig()
    config.read(CONFIG_PATH)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.getint("Servers", "max_workers", fallback=10)))
    raft_pb2_grpc.add_FrontEndServicer_to_server(FrontEnd(), server)
    server.add_insecure_port(f'{config.get("Global", "base_address", fallback="127.0.0.1")}:8001')
    server.start()
    server.wait_for_termination()
