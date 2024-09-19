# Push chunked images to serverless-registry

This is a pretty simple tool that allows you to push docker images to a
registry when the layers are too big.

How to run:

```bash
bun install
```

To run:

```bash
docker tag my-image:latest $IMAGE_URI
echo $PASSWORD | USERNAME=<your-configured-username> bun run index.ts $IMAGE_URI
```
