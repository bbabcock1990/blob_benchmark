import os
import time
import subprocess

# Azure Blob Storage details
STORAGE_ACCOUNT_NAME = "storageblobtest01"
CONTAINER_NAME = "testcontainer"
BLOB_PREFIX = "test_blob"
SAS_TOKEN = ""  # Replace with your SAS token

# Data for testing
DATA_SIZE_MB = 64  # Set file size to 64 MB
NUM_FILES = 1000  # Total number of files to upload
TEST_FILE = "test_data.bin"
AZCOPY_CONCURRENCY = "16"  # Set azcopy concurrency value

# Set the AZCOPY_CONCURRENCY_VALUE environment variable
os.environ["AZCOPY_CONCURRENCY_VALUE"] = AZCOPY_CONCURRENCY

# Generate test data file for streaming
with open(TEST_FILE, "wb") as f:
    f.write(os.urandom(DATA_SIZE_MB * 1024 * 1024))  # Generate 64 MB test file

def azcopy_upload(blob_name):
    """Upload a single blob using azcopy."""
    blob_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{blob_name}"
    command = [
        "azcopy",
        "copy",
        TEST_FILE,
        f"{blob_url}?{SAS_TOKEN}",
        "--overwrite=true"
    ]
    try:
        start_time = time.time()
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return time.time() - start_time
    except subprocess.CalledProcessError as e:
        print(f"Error uploading blob {blob_name}: {e}")
        return None

def azcopy_download(blob_name):
    """Download a single blob using azcopy."""
    blob_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{blob_name}"
    command = [
        "azcopy",
        "copy",
        f"{blob_url}?{SAS_TOKEN}",
        f"downloaded_{blob_name}.bin",
        "--overwrite=true"
    ]
    try:
        start_time = time.time()
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return time.time() - start_time
    except subprocess.CalledProcessError as e:
        print(f"Error downloading blob {blob_name}: {e}")
        return None

# Verbose details
print("===== Azure Blob Storage Performance Test Using AzCopy =====")
print(f"File size: {DATA_SIZE_MB} MB")
print(f"Number of files: {NUM_FILES}")
print(f"AzCopy Concurrency: {AZCOPY_CONCURRENCY}")
print("==============================================\n")

# Measure parallel PUT throughput
print("Starting parallel PUT operations...")
put_start_time = time.time()
for i in range(NUM_FILES):
    blob_name = f"{BLOB_PREFIX}_{i}"
    azcopy_upload(blob_name)
put_end_time = time.time()
total_put_time = put_end_time - put_start_time
avg_put_throughput = (DATA_SIZE_MB * NUM_FILES) / total_put_time
print(f"Total PUT time: {total_put_time:.2f} seconds")
print(f"PUT Throughput: {avg_put_throughput:.2f} MB/s\n")

# Measure parallel GET throughput
print("Starting parallel GET operations...")
get_start_time = time.time()
for i in range(NUM_FILES):
    blob_name = f"{BLOB_PREFIX}_{i}"
    azcopy_download(blob_name)
get_end_time = time.time()
total_get_time = get_end_time - get_start_time
avg_get_throughput = (DATA_SIZE_MB * NUM_FILES) / total_get_time
print(f"Total GET time: {total_get_time:.2f} seconds")
print(f"GET Throughput: {avg_get_throughput:.2f} MB/s\n")

# Cleanup local files
print("Cleaning up local files...")
for i in range(NUM_FILES):
    try:
        os.remove(f"downloaded_{BLOB_PREFIX}_{i}.bin")
        print(f"Deleted local file: downloaded_{BLOB_PREFIX}_{i}.bin")
    except FileNotFoundError:
        print(f"File not found, skipping: downloaded_{BLOB_PREFIX}_{i}.bin")
print("Local file cleanup complete.")
