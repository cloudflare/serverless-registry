# Docker Registry in Workers

This repository contains a docker registry implementation in Workers that uses R2.

It supports all pushing and pulling workflows. It also supports
Username/Password and public key JWT based authentication.

### Deployment

You have to install all the dependencies with your favorite package manager (e.g pnpm, npm, yarn, bun...).
```bash
$ npm install
```

After installation, there is a few steps to actually deploy the registry into production:

1. Setup the R2 Bucket for this registry

```bash
$ wrangler --env production r2 bucket create r2-registry 
```

Add this to your `wrangler.toml`
```
r2_buckets = [
    { binding = "REGISTRY", bucket_name = "r2-registry"}
]
```

2. Setup the KV namespace for this registry

```bash
$ wrangler --env production kv:namespace create r2_registry_uploads 
```

Add the namespace id to your `wrangler.toml`
```
kv_namespaces = [
    { binding = "UPLOADS", id = "<namespace-id-here>"}
]
```

3. Setup the JWT_STATE_SECRET secret binding

```bash
$ echo `node -e "console.log(crypto.randomUUID())"` | wrangler --env production secret put JWT_STATE_SECRET
```

4. Deploy your image registry
```bash
$ wrangler deploy --env production
```

Your registry should be up and running. It will refuse any requests if you don't setup credentials.

### Adding username password based authentication
Set the USERNAME and PASSWORD as secrets with `wrangler secret put USERNAME` and `wrangler secret put PASSWORD`.

### Adding JWT authentication with public key
You can add a base64 encoded JWT public key to verify passwords (or token) that are signed by the private key.
`wrangler secret put JWT_REGISTRY_TOKENS_PUBLIC_KEY`

### Known limitations
Right now there is some limitations with this docker registry.

- Pushing with docker is limited to images that have layers of maximum size 500MB. Refer to maximum request body sizes in your Workers plan.
- To circumvent that limitation, you can manually add the layer and the manifest into the R2 bucket or use a client that is able to chunk uploads in sizes less than 500MB (or the limit that you have in your Workers plan).
- If you use `wrangler dev` and push to the R2 registry with docker, the R2 registry will have to buffer the request on the Worker.

## License

The project is licensed under the [Apache License](https://opensource.org/licenses/apache-2.0/).

### Contribution

See `CONTRIBUTING.md` for contributing to the project.
