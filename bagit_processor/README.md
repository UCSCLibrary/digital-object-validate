# Docker container for BagIt Validation

This container processes BagIt preservation bags, by first validating them against the UCSC profile, and then 
zipping them to S3 via a streaming upload.

To build and update the ECR repository:

```
# Authenticate local Docker daemon with the AWS ECR registry
aws ecr get-login-password --region us-west-2 | \
  docker login --username AWS --password-stdin {account_number}.dkr.ecr.us-west-2.amazonaws.com

# for my system I needed to update an underlying python emulator to work with python:3.11-slim
docker run --privileged --rm tonistiigi/binfmt --install all

# Build the image for ARM64 using Buildx (Crucial for im4gn Graviton instances)
docker buildx build \
    --platform linux/arm64 \
    -t {account_number}.dkr.ecr.us-west-2.amazonaws.com/bagit-processor:latest \
    --push .
```

The image in ECR is a parameter in the Cloudformation stack for this workflow. If the ARN changes it will need
to be updated there.