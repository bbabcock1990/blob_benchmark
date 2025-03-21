import os
import time
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError  # Import specific exception
from concurrent.futures import ThreadPoolExecutor

# Azure Blob Storage details
STORAGE_ACCOUNT_NAME = "storageblobtest01"
STORAGE_ACCOUNT_KEY = ""
CONTAINER_NAME = "testcontainer"
BLOB_PREFIX = "test_blob"

# Data for testing
DATA_SIZE_MB = 64  # Set file size to 64 MB
TEST_DATA = os.urandom(DATA_SIZE_MB * 1024 * 1024)
NUM_THREADS = 10  # Number of parallel threads
MAX_CONCURRENCY = 8  # Max concurrency for Azure SDK operations

# Create BlobServiceClient
blob_service_client = BlobServiceClient(
    f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/",
    credential=STORAGE_ACCOUNT_KEY
)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Ensure the container exists
try:
    container_client.create_container()
except ResourceExistsError:
    pass  # Ignore if container already exists

def upload_blob(blob_name):
    """Upload a single blob using a stream and return the time taken."""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        start_time = time.time()
        with open("test_data.bin", "rb") as data_stream:  # Use a file or stream
            blob_client.upload_blob(
                data_stream,
                overwrite=True,
                max_concurrency=MAX_CONCURRENCY
            )
        put_time = time.time() - start_time
        return put_time
    except Exception as e:
        print(f"Error uploading blob {blob_name}: {e}")
        return None

def download_blob(blob_name):
    """Download a single blob to a stream and return the time taken."""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        start_time = time.time()
        with open(f"downloaded_{blob_name}.bin", "wb") as file_stream:
            download_stream = blob_client.download_blob(max_concurrency=MAX_CONCURRENCY)
            file_stream.write(download_stream.readall())
        get_time = time.time() - start_time
        return get_time
    except Exception as e:
        print(f"Error downloading blob {blob_name}: {e}")
        return None

# Generate test data file for streaming
with open("test_data.bin", "wb") as f:
    f.write(os.urandom(DATA_SIZE_MB * 1024 * 1024))  # Generate 64 MB test file

# Verbose details
print("===== Azure Blob Storage Performance Test =====")
print(f"File size: {DATA_SIZE_MB} MB")
print(f"Number of files: {NUM_THREADS}")
print(f"Number of threads: {NUM_THREADS}")
print(f"Max concurrency: {MAX_CONCURRENCY}")
print("==============================================\n")

# Measure parallel PUT throughput
print("Starting parallel PUT operations...")
put_start_time = time.time()
with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    put_times = list(executor.map(upload_blob, [f"{BLOB_PREFIX}_{i}" for i in range(NUM_THREADS)]))
put_end_time = time.time()
total_put_time = put_end_time - put_start_time
avg_put_throughput = (DATA_SIZE_MB * NUM_THREADS) / total_put_time
print(f"Total PUT time: {total_put_time:.2f} seconds")
print(f"PUT Throughput (parallel): {avg_put_throughput:.2f} MB/s\n")

# Measure parallel GET throughput
print("Starting parallel GET operations...")
get_start_time = time.time()
with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    get_times = list(executor.map(download_blob, [f"{BLOB_PREFIX}_{i}" for i in range(NUM_THREADS)]))
get_end_time = time.time()
total_get_time = get_end_time - get_start_time
avg_get_throughput = (DATA_SIZE_MB * NUM_THREADS) / total_get_time
print(f"Total GET time: {total_get_time:.2f} seconds")
print(f"GET Throughput (parallel): {avg_get_throughput:.2f} MB/s\n")

# Cleanup optimization: Use delete_blobs for batch deletion
def cleanup_blobs(blob_names):
    """Delete multiple blobs in parallel."""
    try:
        container_client.delete_blobs(*blob_names)  # Pass blob names directly
        print("Blobs deleted successfully.")
    except Exception as e:
        print(f"Error during cleanup: {e}")

def cleanup_local_files(file_names):
    """Delete local files created during the download process."""
    for file_name in file_names:
        try:
            os.remove(file_name)
            print(f"Deleted local file: {file_name}")
        except FileNotFoundError:
            print(f"File not found, skipping: {file_name}")
        except Exception as e:
            print(f"Error deleting file {file_name}: {e}")

# Cleanup
print("Cleaning up blobs...")
cleanup_blobs([f"{BLOB_PREFIX}_{i}" for i in range(NUM_THREADS)])
print("Blob cleanup complete.")

print("Cleaning up local files...")
cleanup_local_files([f"downloaded_{BLOB_PREFIX}_{i}.bin" for i in range(NUM_THREADS)])
print("Local file cleanup complete.")
