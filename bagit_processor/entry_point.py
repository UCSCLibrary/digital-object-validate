# This script reads the environment variables passed down by AWS Batch, downloads the folder,
# runs the BagIt validation against the UCSC schema, and streams the uncompressed zip back to S3.

import os
import sys
import boto3
import logging
import requests
import subprocess
import zipfile
import zipstream
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config
from boto3.s3.transfer import TransferConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class StreamAdapter:
    """Adapts a generator/iterable into a file-like object with a read(size) method for Boto3."""
    def __init__(self, iterable):
        self.iterator = iter(iterable)
        self.buffer = b''

    def read(self, size=-1):
        # Read all remaining data
        if size < 0:
            res = self.buffer + b''.join(self.iterator)
            self.buffer = b''
            return res
        
        # Buffer enough data to satisfy the requested size
        while len(self.buffer) < size:
            try:
                self.buffer += next(self.iterator)
            except StopIteration:
                break
        
        # Return the chunk and keep the rest in the buffer
        result = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return result

def main():

    bagit_profile = "ucscbagit-v0.3.json"

    s3 = boto3.client('s3')
    bucket_in = os.environ.get('S3_INPUT_BUCKET')
    bucket_out = os.environ.get('S3_OUTPUT_BUCKET')
    folder = os.environ.get('TARGET_FOLDER')
    output_zip = f"{folder.rstrip('/')}.zip"
    local_dir = f"/mnt/nvme/{folder}" 
    
    os.makedirs(local_dir, exist_ok=True)

    logging.info(f"Downloading s3://{bucket_in}/{folder}")
    try:
        download_folder(bucket_in, folder, local_dir)
        logging.info("Download successful")

    except Exception as e:
        logging.error(f"Download Failed: {str(e)}")
        sys.exit(1)

    logging.info("Beginning BagIt validation...")
    try:
        result = subprocess.run(
            ["python3", "./bagit_profile.py", "--file", bagit_profile, bagit_profile, local_dir],
            check=True,
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            logging.info(result.stdout)
        logging.info("BagIt validation Successful.")
            
    except subprocess.CalledProcessError as e:
        logging.error(f"BagIt validation Failed with exit code {e.returncode}.")
        logging.error(f"Standard Error:\n{e.stderr}")
        if e.stdout:
            logging.info(f"Standard Output:\n{e.stdout}")
        sys.exit(1)

    logging.info(f"Streaming uncompressed zip to s3://{bucket_out}/{output_zip}")
    zs = zipstream.ZipFile(mode='w', compression=zipfile.ZIP_STORED, allowZip64=True)
    for root, _, files in os.walk(local_dir):
        for file in files:
            full_path = os.path.join(root, file)
            arcname = os.path.relpath(full_path, local_dir)
            zs.write(full_path, arcname=arcname)
    
    zip_stream_adapter = StreamAdapter(zs)

    s3.upload_fileobj(
        zip_stream_adapter, 
        bucket_out, 
        output_zip,
        ExtraArgs={"Tagging": "status=validated"} # use a tag for future workflow improvements
    )
    logging.info(f"End processing: {folder}")

def download_folder(bucket, folder, local_dir):
    # Enlarge the pool to accomodate up to 5 concurrent downloads
    boto_config = Config(max_pool_connections=60)
    s3 = boto3.client('s3', config=boto_config)
    
    # Adjust sizing to account for A/V files up to 1 TB
    transfer_config = TransferConfig(
        multipart_threshold=100 * 1024 * 1024,  # S3 downloads have a 10k part limit
        multipart_chunksize=100 * 1024 * 1024,  # 100MB x 10k = 1 TB
        max_concurrency=10,                     # concurrent chunks per file, go easy on old cpu
        use_threads=True
    )

    paginator = s3.get_paginator('list_objects_v2')
    files = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=folder):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('/'): continue # skip empty directories
                
            # Ensure nested folders exist
            rel_path = os.path.relpath(key, folder)
            dest_path = os.path.join(local_dir, rel_path)
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)

            files.append((key, dest_path))

    # Download up to 5 files concurrently using a thread worker pool
    logging.info(f"Found {len(files)} files. Setting up concurrent download.")
    with ThreadPoolExecutor(max_workers=5) as executor:
        tasks = [
            executor.submit(
                s3.download_file, 
                bucket, 
                key, 
                dest_path, 
                Config=transfer_config
            )
            for key, dest_path in files
        ]
        
        # Wait for all downloads to complete
        for task in tasks:
            task.result()

if __name__ == "__main__":
    main()
