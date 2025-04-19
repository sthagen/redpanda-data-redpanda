ARG BASE_IMAGE_OS_NAME=fedora
ARG BASE_IMAGE_OS_VERSION=38
FROM ${BASE_IMAGE_OS_NAME}:${BASE_IMAGE_OS_VERSION}
ARG TARGETARCH

COPY --chown=0:0 bazel/install-deps.sh /

RUN dnf install -y wget || { apt-get update && apt install -y wget; }
# convenience for CI that runs sysctl to bump max-aio-nr for testing
# ubuntu already has this installed by default
RUN dnf install -y procps-ng || true
RUN dnf install -y python3 || { apt-get update && apt install -y python3 python3-venv; }
RUN wget -O /usr/local/bin/bazel \
        https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-${TARGETARCH} && \
        chmod +x /usr/local/bin/bazel

# run after wget installation to take advantage of pkg cache cleaning
RUN CLEAN_PKG_CACHE=true /install-deps.sh && rm /install-deps.sh

# CI will run this container as root, but a non-root user will clone the repo and set it up,
# so we should just ignore these warnings for now.
RUN git config --global --add safe.directory '*'
