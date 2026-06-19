# Docker Push for Serverless Registry (Go)

A Go-based CLI tool for pushing Docker images to Cloudflare serverless-registry, with support for large layers that exceed the 500 MB blob upload limit.

## Features

- Handles Docker images with layers larger than 500 MB
- Automatic chunked uploads based on `oci-chunk-max-length` header
- Concurrent layer uploads for faster performance
- Retry logic for failed uploads
- Support for both HTTP and HTTPS protocols
- Compatible with Basic authentication

## How It Works

1. Exports the Docker image using `docker save`
2. Extracts and compresses each layer to gzip format
3. Calculates SHA256 digests for all layers
4. Uploads layers in chunks to respect registry size limits
5. Creates and uploads the OCI manifest

The tool reads the `oci-chunk-max-length` header from the registry to determine the maximum chunk size for uploads, making it compatible with Cloudflare Workers' 500 MB request body limitation.

## Installation

### Build from source:

```bash
go build -o docker-push
```

### Install to your Go bin:

```bash
go install
```

## Usage

### Basic usage:

```bash
echo $PASSWORD | ./docker-push -username $USERNAME_REGISTRY <image-uri>
```

### Example with Cloudflare registry:

```bash
# Tag your image
docker tag pytorch:latest my-registry.example.com/pytorch:latest

# Push the image
echo "my-password" | ./docker-push -username myuser my-registry.example.com/pytorch:latest
```

### Command-line flags:

- `-username` - Registry username (can also be set via `USERNAME_REGISTRY` env var)
- `-insecure` - Use HTTP instead of HTTPS (default: false, can be set via `INSECURE_HTTP_PUSH=true`)
- `-max-retries` - Maximum number of retries per layer (default: 3)
- `-concurrency` - Number of concurrent uploads (default: 5)
- `-cache-dir` - Cache directory for compressed layers (default: `.cache`)
- `-image-dir` - Directory to extract image tar (default: `.output-image`)

### Example with custom settings:

```bash
echo "password" | ./docker-push \
  -username myuser \
  -concurrency 10 \
  -max-retries 5 \
  my-registry.example.com/large-image:latest
```

## Pushing to localhost registry

To push to a localhost registry, use the `-insecure` flag:

```bash
echo "password" | ./docker-push -insecure -username myuser localhost:5000/myimage:latest
```

Or set the environment variable:

```bash
export INSECURE_HTTP_PUSH=true
echo "password" | ./docker-push -username myuser localhost:5000/myimage:latest
```

## Testing with PyTorch Image

```bash
# Pull PyTorch image if you don't have it locally
docker pull pytorch/pytorch:latest

# Tag it for your registry
docker tag pytorch/pytorch:latest your-registry.example.com/pytorch:latest

# Push using the tool
echo "your-password" | ./docker-push -username your-username your-registry.example.com/pytorch:latest
```

## Cache and Output Directories

The tool creates two directories during operation:

- `.cache/` - Stores compressed layers and digest pointers
- `.output-image/` - Contains the extracted Docker image tar

These directories are reused across multiple pushes to avoid recompressing layers that haven't changed.

## Architecture

The Go implementation mirrors the TypeScript/Bun version but with the following improvements:

- Native Go concurrency with goroutines and channels
- Type-safe implementation
- Better memory management for large files
- No dependency on external fetch libraries
- Cross-platform binary support

## Comparison with TypeScript Version

**Similarities:**
- Same chunking algorithm
- Same OCI manifest format
- Compatible with the same registry endpoints

**Differences:**
- Uses Go's native `net/http` instead of node-fetch
- Concurrency controlled with goroutines and semaphores
- Better type safety with Go's type system
- Single binary distribution (no runtime needed)

## Troubleshooting

### "Image doesn't exist or docker daemon is not running"

Make sure Docker is running and the image exists locally:
```bash
docker images
```

### Authentication errors

Ensure your username and password are correct and that you're passing the password via stdin:
```bash
echo "password" | ./docker-push -username user <image>
```

### Upload failures

Try increasing the retry count:
```bash
echo "password" | ./docker-push -max-retries 10 -username user <image>
```

## License

Same as the parent serverless-registry project.
