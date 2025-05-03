## tests/docker

This Dockerfile is used to back the ducktape cluster nodes when running ducktape in docker/podman mode. It relies heavily on multi-root builds to build the relatively heavy dependencies (mostly large Java projects) in parallel and to limit layer rebuilds when only a subset change.

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
