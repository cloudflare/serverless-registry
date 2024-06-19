## Running the registry


Running the registry is done via `wrangler dev`

```bash
$ npx wrangler --env dev dev
```

You can `docker login` locally for running a push

```bash
$ docker login localhost:8787 # Or the URL that your worker is listening in
```

By default in a local environment, the registry uses USERNAME=hello and PASSWORD=world. See `wrangler.toml` for more details.

Tag any image that you have locally and push it:

```bash
$ docker tag my-image:local docker tag localhost:8787/my-image:local && docker push localhost:8787/my-image:local
```

## Testing

Test the registry with unit tests by running `vitest` with `miniflare 2`. 

```bash
$ pnpm test
```

## Making a change

Fork the repository and make changes on a new branch. Then after you're done, submit a PR so the maintainers can review it.
