#!/usr/bin/env python3
"""
Standalone test script for HDFS server.
Tests Upload, Download, and List operations.

Usage:
    # Terminal 1: Start HDFS server
    python hdfs/server.py
    
    # Terminal 2: Run tests
    python test_hdfs.py
"""

import os
import sys
import grpc
import tempfile
import shutil

# Add hdfs directory to path to import generated protobuf files
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'hdfs'))
import hdfs_pb2
import hdfs_pb2_grpc

HDFS_ADDR = os.environ.get("HDFS_ADDR", "localhost:50051")

def test_upload(stub):
    """Test uploading a file to HDFS"""
    print("=" * 60)
    print("TEST 1: Upload")
    print("=" * 60)
    
    test_path = "test/upload_test.txt"
    test_data = b"Hello, HDFS! This is a test file."
    
    print(f"Uploading to: {test_path}")
    print(f"Data: {test_data.decode()}")
    
    resp = stub.Upload(hdfs_pb2.UploadRequest(path=test_path, data=test_data))
    
    if resp.success:
        print(f"✅ Upload successful: {resp.message}")
        return True
    else:
        print(f"❌ Upload failed: {resp.message}")
        return False

def test_download(stub):
    """Test downloading a file from HDFS"""
    print("\n" + "=" * 60)
    print("TEST 2: Download")
    print("=" * 60)
    
    test_path = "test/upload_test.txt"
    expected_data = b"Hello, HDFS! This is a test file."
    
    print(f"Downloading from: {test_path}")
    
    resp = stub.Download(hdfs_pb2.DownloadRequest(path=test_path))
    
    if resp.success:
        print(f"✅ Download successful: {resp.message}")
        print(f"Received data: {resp.data.decode()}")
        if resp.data == expected_data:
            print("✅ Data matches expected content!")
            return True
        else:
            print(f"❌ Data mismatch! Expected: {expected_data}, Got: {resp.data}")
            return False
    else:
        print(f"❌ Download failed: {resp.message}")
        return False

def test_download_nonexistent(stub):
    """Test downloading a non-existent file"""
    print("\n" + "=" * 60)
    print("TEST 3: Download Non-existent File")
    print("=" * 60)
    
    test_path = "test/nonexistent.txt"
    
    print(f"Attempting to download: {test_path}")
    
    resp = stub.Download(hdfs_pb2.DownloadRequest(path=test_path))
    
    if not resp.success:
        print(f"✅ Correctly returned failure: {resp.message}")
        return True
    else:
        print(f"❌ Should have failed but didn't!")
        return False

def test_list_directory(stub):
    """Test listing directory contents"""
    print("\n" + "=" * 60)
    print("TEST 4: List Directory")
    print("=" * 60)
    
    # First, upload a few files to create a directory structure
    files = {
        "test/dir/file1.txt": b"File 1 content",
        "test/dir/file2.txt": b"File 2 content",
        "test/dir/file3.txt": b"File 3 content",
    }
    
    print("Uploading test files...")
    for path, data in files.items():
        resp = stub.Upload(hdfs_pb2.UploadRequest(path=path, data=data))
        if resp.success:
            print(f"  ✅ Uploaded: {path}")
        else:
            print(f"  ❌ Failed to upload: {path} - {resp.message}")
            return False
    
    # Now list the directory
    print(f"\nListing directory: test/dir")
    resp = stub.List(hdfs_pb2.ListRequest(path="test/dir"))
    
    if resp.entries:
        print(f"✅ List successful: {resp.message}")
        print(f"Found {len(resp.entries)} entries:")
        for entry in sorted(resp.entries):
            print(f"  - {entry}")
        
        expected_files = {"file1.txt", "file2.txt", "file3.txt"}
        found_files = set(resp.entries)
        if found_files == expected_files:
            print("✅ All expected files found!")
            return True
        else:
            print(f"❌ File mismatch! Expected: {expected_files}, Got: {found_files}")
            return False
    else:
        print(f"❌ List failed or empty: {resp.message}")
        return False

def test_list_nonexistent(stub):
    """Test listing a non-existent directory"""
    print("\n" + "=" * 60)
    print("TEST 5: List Non-existent Directory")
    print("=" * 60)
    
    test_path = "nonexistent/directory"
    
    print(f"Attempting to list: {test_path}")
    
    resp = stub.List(hdfs_pb2.ListRequest(path=test_path))
    
    if not resp.entries and "Not found" in resp.message:
        print(f"✅ Correctly returned empty list: {resp.message}")
        return True
    else:
        print(f"❌ Unexpected response: {resp.message}, entries: {resp.entries}")
        return False

def test_path_normalization(stub):
    """Test that paths with leading slashes are handled correctly"""
    print("\n" + "=" * 60)
    print("TEST 6: Path Normalization")
    print("=" * 60)
    
    # Upload with leading slash
    path_with_slash = "/test/normalize_test.txt"
    path_without_slash = "test/normalize_test.txt"
    test_data = b"Normalization test"
    
    print(f"Uploading with leading slash: {path_with_slash}")
    resp1 = stub.Upload(hdfs_pb2.UploadRequest(path=path_with_slash, data=test_data))
    
    if not resp1.success:
        print(f"❌ Upload with slash failed: {resp1.message}")
        return False
    
    print(f"✅ Upload with slash successful")
    
    # Download without leading slash (should find the same file)
    print(f"Downloading without leading slash: {path_without_slash}")
    resp2 = stub.Download(hdfs_pb2.DownloadRequest(path=path_without_slash))
    
    if resp2.success and resp2.data == test_data:
        print(f"✅ Download without slash successful - paths normalized correctly!")
        return True
    else:
        print(f"❌ Path normalization failed!")
        return False

def main():
    print("\n" + "=" * 60)
    print("HDFS Server Standalone Test Suite")
    print("=" * 60)
    print(f"Connecting to HDFS server at: {HDFS_ADDR}")
    print("\nMake sure the HDFS server is running!")
    print("Start it with: python hdfs/server.py")
    print("\nPress Enter to continue or Ctrl+C to cancel...")
    try:
        input()
    except KeyboardInterrupt:
        print("\nCancelled.")
        return
    
    # Create a temporary directory for testing if using local filesystem
    # (In Docker, this would be /data)
    test_root = os.path.join(tempfile.gettempdir(), "hdfs_test_data")
    if os.path.exists(test_root):
        shutil.rmtree(test_root)
    os.makedirs(test_root, exist_ok=True)
    print(f"\nUsing test root: {test_root}")
    print("(In production, this would be /data in the Docker container)\n")
    
    try:
        # Connect to HDFS server
        channel = grpc.insecure_channel(HDFS_ADDR)
        stub = hdfs_pb2_grpc.HdfsServiceStub(channel)
        
        # Test connection
        print("Testing connection...")
        try:
            # Try a simple operation to test connection
            resp = stub.List(hdfs_pb2.ListRequest(path=""))
            print("✅ Connected to HDFS server!\n")
        except grpc.RpcError as e:
            print(f"❌ Failed to connect to HDFS server: {e.details()}")
            print(f"\nMake sure the HDFS server is running on {HDFS_ADDR}")
            return
        
        # Run all tests
        results = []
        results.append(("Upload", test_upload(stub)))
        results.append(("Download", test_download(stub)))
        results.append(("Download Non-existent", test_download_nonexistent(stub)))
        results.append(("List Directory", test_list_directory(stub)))
        results.append(("List Non-existent", test_list_nonexistent(stub)))
        results.append(("Path Normalization", test_path_normalization(stub)))
        
        # Print summary
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        passed = sum(1 for _, result in results if result)
        total = len(results)
        
        for test_name, result in results:
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status}: {test_name}")
        
        print(f"\nTotal: {passed}/{total} tests passed")
        
        if passed == total:
            print("\n🎉 All tests passed!")
            return 0
        else:
            print(f"\n⚠️  {total - passed} test(s) failed")
            return 1
            
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        # Cleanup test directory
        if os.path.exists(test_root):
            shutil.rmtree(test_root)

if __name__ == "__main__":
    sys.exit(main())

