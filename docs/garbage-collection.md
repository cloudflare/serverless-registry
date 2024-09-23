# Removing manifests and garbage collection

Garbage collection is useful due to how [OCI](https://github.com/opencontainers/image-spec/blob/main/manifest.md) container images get shipped to registries.

```json
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {
    "mediaType": "application/vnd.oci.image.config.v1+json",
    "digest": "sha256:b5b2b2c507a0944348e0303114d8d93aaaa081732b86451d9bce1f432a537bc7",
    "size": 7023
  },
  "layers": [
    {
      "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
      "digest": "sha256:9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0",
      "size": 32654
    },
    {
      "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
      "digest": "sha256:3c3a4604a545cdc127456d94e421cd355bca5b528f4a9c1905b15da2eb4a4c6b",
      "size": 16724
    },
    {
      "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
      "digest": "sha256:ec4b8955958665577945c89419d1af06b5f7636b4ac3da7f12184802ad867736",
      "size": 73109
    }
  ]
}
```

This is how a container image manifest looks, it's a "tree-like" structure where the manifest references
layers. If you remove an image from a registry, you're probably just removing its manifest. However, layers
will still be around taking space.

## Removing an image and triggering the garbage collection

To delete an image of your registry, you can use `skopeo delete` or an API call:

```
# If you pushed to serverless.workers.dev/my-image:latest
curl -X DELETE -X "Authorization: $CREDENTIAL" https://serverless.workers.dev/my-image/manifests/latest
# You will also need to remove the digest reference
curl -X DELETE -X "Authorization: $CREDENTIAL" https://serverless.workers.dev/my-image/manifests/<digest>
```

The layer still exists in the registry, but we can remove it by triggering the garbage collector.

```
curl -X POST -H "Authorization: $CREDENTIAL" https://serverless.workers.dev/my-image/gc
{"success":true}
```

## How does it work

How do we remove them? We take the approach of listing all manifests in a namespace and storing its digests
in a Set, then we list all the layers and those that are not in the Set get removed. That has a big drawback
that means we might be removing layers that don't have a manifest but are about to have one at the end of their push.

In serverless-registry, if we remove a layer garbage collecting the manifest endpoint will throw a BLOB_UNKNOWN
error, but the garbage collector can still race with that endpont, so we go back to square one.

Some registries take a lock stop the world approach, however serverless-registry can't really do that due
to its objective of only using R2. However, we need to fail whenever a race condition happens, a data
race that causes data-loss would be completely unacceptable.

That's when we introduce a simple system where instead of taking a lock, we mark in R2
that we are about to create a manifest and that we are inserting data.
If the garbage collector starts and sees that key, it will fail. At the end of the insertion, the insertion mark
gets updated.

The same goes for the garbage collector, when it starts it creates a mark, and when it finishes it updates the
mark.

Let's state some scenarios:

```
PutManifest                       GC
1. markForInsertion()           2. markForGarbageCollection()
...
3. checkLayersExist()           ...
4. checkGCDidntStart() // fails due to ongoing gc
5. insertManifest()
```

```
PutManifest                       GC
1. markForInsertion()           4. markForGarbageCollection()
...
2. checkLayersExist()           6. mark = getInsertionMark();
3. checkGCDidntStart()          7. ... finds a layer to remove
5. insertManifest()             8. checkOnGoingUpdates() // fails due to ongoing updates
9. unmarkForInsertion()
```

```
PutManifest                       GC
1. markForInsertion()           4. markForGarbageCollection()
...
2. checkLayersExist()           6. mark = getInsertionMark();
3. checkGCDidntStart()          7. ... finds a layer to remove
5. insertManifest()             9. checkOnGoingUpdates()
8. unmarkForInsertion()         10. checkMark(mark) // this fails, not latest, can't delete layer
```

```
PutManifest                           GC
4. markForInsertion()                 1. markForGarbageCollection()
5. gcMark = getGCMark()
6. checkLayersExist()                 2. mark = getInsertionMark();
                                      3. checkOngoingUpdates() and checkMark(mark)
                                      7. deleteLayer() and unmarkGarbageCollector();
8. checkGCDidntStart(gcMark) // fails because latest gc marker is different
```

It's a pattern where you build the state you need a lock in, get the mark of when you built that world,
and confirm before making changes from that view that there is nothing that might've changed the view.
