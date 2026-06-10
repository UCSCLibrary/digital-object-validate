# This script reads the environment variables passed down by AWS Batch, downloads the folder,
# runs the BagIt validation against the UCSC schema, and streams the uncompressed zip back to S3.

import os
import sys
import boto3
import requests
import zipfile
import zipstream
from concurrent.futures import ThreadPoolExecutor
from boto3.s3.transfer import TransferConfig

def main():

    bagit_profile = "ucscbagit-v0.3.json"

    s3 = boto3.client('s3')
    bucket_in = os.environ.get('S3_INPUT_BUCKET')
    bucket_out = os.environ.get('S3_OUTPUT_BUCKET')
    folder = os.environ.get('TARGET_FOLDER')
    output_zip = f"{folder.rstrip('/')}.zip"
    local_dir = "/mnt/nvme/target_data"
    
    os.makedirs(local_dir, exist_ok=True)

    print(f"Downloading s3://{bucket_in}/{folder} to {local_dir}...")
    try:
        download_folder(bucket_in, folder, local_dir)
        print("Download successful")

    except Exception as e:
        print(f"Download Failed: {str(e)}")
        sys.exit(1)

    print("Beginning BagIt validation...")
    try:
        result = subprocess.run(
            ["python3", "./bagit_profile.py", "--file", bagit_profile, bagit_profile, local_dir],
            check=True,
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            print(result.stdout)
        print("Validation Successful.")
            
    except subprocess.CalledProcessError as e:
        print(f"Validation Failed with exit code {e.returncode}.")
        print(f"Standard Error:\n{e.stderr}")
        if e.stdout:
            print(f"Standard Output:\n{e.stdout}")
        sys.exit(1)

    print(f"Streaming uncompressed zip to s3://{bucket_out}/{output_zip}...")
    zs = zipstream.ZipFile(mode='w', compression=zipfile.ZIP_STORED)
    for root, _, files in os.walk(local_dir):
        for file in files:
            full_path = os.path.join(root, file)
            arcname = os.path.relpath(full_path, local_dir)
            zs.write(full_path, arcname=arcname)
            
    # Multipart upload stream with a 'Pending' tracking tag for your manual review system
    s3.upload_fileobj(
        zs, 
        bucket_out, 
        output_zip,
        ExtraArgs={"Tagging": "ReviewStatus=Pending"}
    )
    print("Process Complete.")

def download_folder(bucket, folder, local_dir):
    s3 = boto3.client('s3')
    
    # Optimize the transfer for the NVMe/25Gbps network combo on im4gn.large
    transfer_config = TransferConfig(
        max_concurrency=10,
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

    # Download files concurrently using a thread worker pool
    print(f"Spinning up worker pool to download {len(files)} files...")
    with ThreadPoolExecutor(max_workers=8) as executor:
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
