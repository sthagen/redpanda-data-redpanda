## tests/docker

This Dockerfile is used to back the ducktape cluster nodes when running ducktape in docker mode. It relies heavily on multi-root builds to build the relatively heavy dependencies (mostly large Java projects) in parallel and to limit layer rebuilds when only a subset change.

## Caching

### Cache mounts in docker

The docker image build uses multiple caches of type `--mount=type=cache`: these caches are mounts which are stored separately by docker, are not included in the layer hash, do not affect caching on the layer (i.e., a cached layer may be re-used even if the cache mount contents have changed) and which do not propagate to later RUN commands nor the final image. The same mount is provided to all commands with the same `id` regardless of the dockerfile being run (or what layer is being built). They can be used to cache files whose presence or absence does not affect the outcome of a RUN command, and which may allow the RUN to complete more quickly if cache hits occur. For example, downloading a tar file (which does not change at the source) may first check in the cache to see if the tar already exists.

You can look at your current cache mounts using `docker builder du --filter 'type=exec.cachemount' --verbose`, which gives output like this:

```
ID:             m14sema73ey9iwkwquskdjh8p
Created at:     2025-04-28 18:33:19.198371074 +0000 UTC
Mutable:        true
Reclaimable:    true
Shared:         false
Size:           4.328GB
Description:    cached mount /dl-cache from exec /bin/sh -c /keycloak && rm /keycloak
Usage count:    77
Last used:      13 seconds ago
Type:           exec.cachemount

ID:             65cc6wnfelmodpe8s6dx5sn58
Created at:     2025-05-02 18:43:54.776165542 +0000 UTC
Mutable:        true
Reclaimable:    true
Shared:         false
Size:           2.783GB
Description:    cached mount /root/.gradle from exec /bin/sh -c /nessie && rm /nessie with id "/gradle"
Usage count:    10
Last used:      About a minute ago
Type:           exec.cachemount
```

This shows the "dl-cache" and "gradle" cache mounts.

You can also examine the contents and summary statistics for all cache mounts using:

```
task rp:build-test-docker-image EXTRA_BUILD_ARGS="--target=debug-caches --progress=plain --build-arg=CACHE_BREAK=$RANDOM"
```


#### Cleanup

In general, cache mounts can use much less space over time than layers which download the same files, since the cache mounts are mutable singletons which contain only the most recent file at a given path, while a given layer may be duplicated many times (including the downloaded files) if any of its dependencies change.

Still, you may want to clear the cache mount, which can be accomplished by building a specific `clean-dl-cache` image like so:

```
task rp:build-test-docker-image EXTRA_BUILD_ARGS="--target=clean-dl-cache --progress=plain --build-arg=CACHE_BREAK=$RANDOM"
```

You may also prune runing the _entire_ builder cache, which also clears cache mounts, like so:

```
docker builder prune --all
```

Specific caches are documented below.


### dl-cache cache mount

This is a cache mount which caches downloaded files. Use the utility functions in `ducktape-deps/download-cache` to interact with it in other ducktape scripts, for example from [ducktape-deps/java-dev-tools]:

```
source "$(dirname "${BASH_SOURCE[0]}")/download-utils" # import download-utils

...

download_extract $java22_url .
```

This downloads `$java22_url` and unpacks it to the current working directory. Caching is handled behind the scenes.

The caching happens automatically: the URL is downloaded into the cache mount at a path like `/dl-cache/<url-hash>/<url-basename>` where `<url-hash>` is hash of the URL passed to the `download*` method (not of the _contents_ of the URL, just of the URL string itself). This ensures that different URLs get different cache directories.

If the contents at a given URL change upstream, the old contents will continue to be used as long as the cache mount remains populated. This is not all that different than the docker behavior in any case: a layer whose input is textually unchanged (i.e., unchanged download URL) may simply be reused from the cache. The cache mount simply extends the period of time during which a re-download will not occur. We should try to always use "immutable" download targets whose contents do not change.



### .m2 caching

#### Design

The test image contains several Java projects which use maven, and if not handled specially the download time for all the maven artifacts can be very long.

To solve this, we seed the java maven stages with an ~/.m2 directory (this is
where maven stores its local repository of downloaded files) downloaded from our
artifact repository. This happens in the m2_seed and java_base stages in the
dockerfile. The contents of this seed cache do not need to be exact: the other stages in the file will work fine if it is empty or contain old jars, etc: as maven already separates files by version and will download any additional files.

When the seed cache is up-to-date, there will be very little maven activity visible during the build. Over time, however, the seed cache may become out of date as project versions are updated and they pull in newer dependency versions. Furthermore, some projects may have -SNAPSHOT dependencies which mean that the jar version may change even if the project and pom.xml is unchanged. So it is useful to periodically refresh the seed cache as described below.

If you regularly see a lot of maven download activity during the build, it may be time to update the seed tarball. Maven activity looks like this:

```
 => => # Downloading from central: https://repo.maven.apache.org/maven2/org/codehaus/plexus/plexus-containers/1.6/plexus-containers-1.6.pom
 => => # Downloaded from central: https://repo.maven.apache.org/maven2/org/codehaus/plexus/plexus-containers/1.6/plexus-containers-1.6.pom (3.8 kB at 92 kB/s)
 => => # Downloading from confluent: https://packages.confluent.io/maven/org/codehaus/plexus/plexus/3.3.2/plexus-3.3.2.pom
 => => # Downloading from central: https://repo.maven.apache.org/maven2/org/codehaus/plexus/plexus/3.3.2/plexus-3.3.2.pom
 => => # Downloaded from central: https://repo.maven.apache.org/maven2/org/codehaus/plexus/plexus/3.3.2/plexus-3.3.2.pom (22 kB at 525 kB/s)
```

The next section describes how to do this:

#### Refresh the seed cache


To update the tarball run:

```
task rp:extract-test-docker-image-m2-tarball
```

and then follow the instructions (prefixed with TODO) emitted to upload the new tarball to our artifact archive.
