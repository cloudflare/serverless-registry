# Container Registry in Workers

This repository contains a container registry implementation in Workers that uses R2.

It supports all pushing and pulling workflows. It also supports
Username/Password and public key JWT based authentication.

### Deployment

You have to install all the dependencies with [pnpm](https://pnpm.io/installation) (other package managers may work, but only pnpm is supported.)

```bash
$ pnpm install
```

After installation, there is a few steps to actually deploy the registry into production:

1. Have your own `wrangler` file.

```bash
$ cp wrangler.toml.example wrangler.toml
```

2. Setup the R2 Bucket for this registry

```bash
$ npx wrangler --env production r2 bucket create r2-registry
```

Add this to your `wrangler.toml`

```
r2_buckets = [
    { binding = "REGISTRY", bucket_name = "r2-registry"}
]
```

3. Deploy your image registry

```bash
$ npx wrangler deploy --env production
```

Your registry should be up and running. It will refuse any requests if you don't setup credentials.

### Adding username password based authentication

Set the USERNAME and PASSWORD as secrets with `npx wrangler secret put USERNAME --env production` and `npx wrangler secret put PASSWORD --env production`.

### Adding JWT authentication with public key

You can add a base64 encoded JWT public key to verify passwords (or token) that are signed by the private key.
`npx wrangler secret put JWT_REGISTRY_TOKENS_PUBLIC_KEY --env production`

### Using with Docker

You can use this registry with Docker to push and pull images.

Example using `docker push` and `docker pull`:

```bash
export REGISTRY_URL=your-url-here

# Replace $PASSWORD and $USERNAME with the actual credentials
echo $PASSWORD | docker login --username $USERNAME --password-stdin $REGISTRY_URL
docker pull ubuntu:latest
docker tag ubuntu:latest $REGISTRY_URL/ubuntu:latest
docker push $REGISTRY_URL/ubuntu:latest

# Check that pulls work
docker rmi ubuntu:latest $REGISTRY_URL/ubuntu:latest
docker pull $REGISTRY_URL/ubuntu:latest
```

### Configuring Pull fallback

You can configure the R2 registry to fallback to another registry if
it doesn't exist in your R2 bucket. It will download from the registry
and copy it into the R2 bucket. In the next pull it will be able to pull it directly from R2.

This is very useful for migrating from one registry to `serverless-registry`.

It supports both Basic and Bearer authentications as explained in the
[registry spec](https://distribution.github.io/distribution/spec/auth/token/).

In the wrangler.toml file:

```
[env.production]
REGISTRIES_JSON = "[{ \"registry\": \"https://url-to-other-registry\", \"password_env\": \"REGISTRY_TOKEN\", \"username\": \"username-to-use\" }]"
```

Set as a secret the registry token of the registry you want to setup
pull fallback in.

For example [gcr](https://cloud.google.com/artifact-registry/docs/reference/docker-api):

```
cat ./registry-service-credentials.json | base64 | wrangler --env production secrets put REGISTRY_TOKEN
```

[Github](https://github.com/settings/tokens) for example uses a simple token that you can copy.

```
echo $GITHUB_TOKEN | wrangler --env production secrets put REGISTRY_TOKEN
```

The trick is always looking for how you would login in Docker for
the target registry and setup the credentials.

**Never put a registry password/token inside the wrangler.toml, please always use `wrangler secrets put`**

### Known limitations

Right now there is some limitations with this container registry.

- Pushing with docker is limited to images that have layers of maximum size 500MB. Refer to maximum request body sizes in your Workers plan.
- To circumvent that limitation, you can either manually interact with the R2 bucket to upload the layer or take a
  peek at the `./push` folder for some inspiration on how can you push big layers.
- If you use `npx wrangler dev` and push to the R2 registry with docker, the R2 registry will have to buffer the request on the Worker.

## License

The project is licensed under the [Apache License](https://opensource.org/licenses/apache-2.0/).

### Contribution

See `CONTRIBUTING.md` for contributing to the project.
