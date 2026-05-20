#!/bin/bash
# Setup script for Assignment 1 testing

echo "Setting up Assignment 1 test environment..."

# Install required packages
echo "Installing grpcio-tools..."
pip install grpcio grpcio-tools

# Check if raft.proto exists
if [ ! -f "raft.proto" ]; then
    echo "ERROR: raft.proto not found!"
    echo "Please place the provided raft.proto file in this directory."
    exit 1
fi

# Generate protobuf files
echo "Generating protobuf files..."
python -m grpc_tools.protoc --python_out=. --grpc_python_out=. raft.proto

if [ $? -eq 0 ]; then
    echo "✓ Generated raft_pb2.py and raft_pb2_grpc.py"
else
    echo "✗ Failed to generate protobuf files"
    exit 1
fi

# Check if required files exist
echo "Checking required files..."
required_files=("frontend.py" "server.py" "config.ini")
for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        echo "✓ Found $file"
    else
        echo "✗ Missing $file"
    fi
done

echo ""
echo "Setup complete! To run tests:"
echo "1. Start frontend: python frontend.py"
echo "2. Run tests: python assignment1_test.py"