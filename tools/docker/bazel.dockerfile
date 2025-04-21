ARG BASE_IMAGE_OS_NAME=ubuntu
ARG BASE_IMAGE_OS_VERSION=noble
FROM ${BASE_IMAGE_OS_NAME}:${BASE_IMAGE_OS_VERSION}
ARG TARGETARCH

COPY --chown=0:0 bazel/install-deps.sh /

RUN apt-get update && apt install -y wget python3 python3-venv curl pigz patchelf devscripts debhelper
RUN wget -O /usr/local/bin/bazel \
        https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-${TARGETARCH} && \
        chmod +x /usr/local/bin/bazel

# run after wget installation to take advantage of pkg cache cleaning
RUN CLEAN_PKG_CACHE=true /install-deps.sh && rm /install-deps.sh

# CI will run this container as root, but a non-root user will clone the repo and set it up,
# so we should just ignore these warnings for now.
RUN git config --global --add safe.directory '*'

# task shell emulation doesn't implement hash but this is needed for venvs
# Provide a wrapper, fedora does this out of the box but ubuntu doesn't
RUN printf '#!/usr/bin/bash\nbuiltin hash "$@"\n' > /usr/bin/hash && chmod +x /usr/bin/hash
