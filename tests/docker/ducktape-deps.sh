#!/bin/bash

set -e

function install_java_client_deps() {
  apt update
  apt install -y \
    build-essential \
    default-jdk \
    git \
    maven
}

function install_system_deps() {
  apt-get update
  apt-get install -qq \
    bind9-utils \
    bind9-dnsutils \
    bsdmainutils \
    curl \
    dmidecode \
    cmake \
    krb5-admin-server \
    krb5-kdc \
    krb5-user \
    iproute2 \
    iptables \
    libatomic1 \
    libyajl-dev \
    libsasl2-dev \
    libsasl2-modules-gssapi-mit \
    libssl-dev \
    net-tools \
    lsof \
    pciutils \
    nodejs \
    npm \
    openssh-server \
    netcat-openbsd \
    sudo \
    llvm \
    python3-pip
}

function install_omb() {
  git -C /opt clone https://github.com/redpanda-data/openmessaging-benchmark.git
  cd /opt/openmessaging-benchmark
  git reset --hard 2674d62ca2b6fd7f22536e924c0df8a8fa21350d
  mvn clean package -DskipTests
}

function install_kafka_tools() {
  for ver in "2.3.1" "2.4.1" "2.5.0" "2.7.0" "3.0.0"; do
    mkdir -p "/opt/kafka-${ver}" && chmod a+rw "/opt/kafka-${ver}" && curl -s "$KAFKA_MIRROR/kafka_2.12-${ver}.tgz" | tar xz --strip-components=1 -C "/opt/kafka-${ver}"
  done
  ln -s /opt/kafka-3.0.0/ /opt/kafka-dev
}

function install_librdkafka() {
  mkdir /opt/librdkafka
  curl -SL "https://github.com/edenhill/librdkafka/archive/v1.9.2.tar.gz" | tar -xz --strip-components=1 -C /opt/librdkafka
  cd /opt/librdkafka
  ./configure
  make -j$(nproc)
  make install
  cd /opt/librdkafka/tests
  make build -j$(nproc)
}

function install_kcat() {
  mkdir /tmp/kcat
  curl -SL "https://github.com/edenhill/kcat/archive/1.7.0.tar.gz" | tar -xz --strip-components=1 -C /tmp/kcat
  cd /tmp/kcat
  ./configure
  make -j$(nproc)
  make install
  ldconfig
}

function install_golang() {
  mkdir -p /usr/local/go/
  if [ $(uname -m) = "aarch64" ]; then
    export ARCHID="arm64"
  else
    export ARCHID="amd64"
  fi
  curl -sSLf --retry 3 --retry-connrefused --retry-delay 2 "https://golang.org/dl/go1.19.2.linux-${ARCHID}.tar.gz" | tar -xz -C /usr/local/go/ --strip 1
}

function install_kaf() {
  go install github.com/birdayz/kaf/cmd/kaf@v0.2.3
  mv /root/go/bin/kaf /usr/local/bin/
}

function install_client_swarm() {
  dir="$1"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  export PATH="$dir/.cargo/bin:${PATH}"
  pushd /tmp
  git clone https://github.com/redpanda-data/client-swarm.git
  pushd client-swarm
  git reset --hard 9ef8e93 &&
    cargo build --release &&
    cp target/release/client-swarm /usr/local/bin &&
    popd
  rm -rf client-swarm
  popd
  rm -rf $dir/.cargo
}

function install_sarama_examples() {
  git -C /opt clone -b v1.32.0 --single-branch https://github.com/Shopify/sarama.git
  cd /opt/sarama/examples/interceptors && go mod tidy && go build
  cd /opt/sarama/examples/http_server && go mod tidy && go build
  cd /opt/sarama/examples/consumergroup && go mod tidy && go build
  cd /opt/sarama/examples/sasl_scram_client && go mod tidy && go build
}

function install_franz_bench() {
  git -C /opt clone -b v1.5.0 --single-branch https://github.com/twmb/franz-go.git
  cd /opt/franz-go
  cd /opt/franz-go/examples/bench && go mod tidy && go build
}

function install_kcl() {
  go install github.com/twmb/kcl@v0.8.0
  mv /root/go/bin/kcl /usr/local/bin/
}

function install_kgo_verifier() {
  git -C /opt clone https://github.com/redpanda-data/kgo-verifier.git &&
    cd /opt/kgo-verifier && git reset --hard a2b3ae780b0dc0bc1b4cf2aa33a9d43d10578b0b &&
    go mod tidy && make
}

function install_addr2line() {
  mkdir -p /opt/scripts &&
    curl https://raw.githubusercontent.com/redpanda-data/seastar/2a9504b3238cba4150be59353bf8d0b3a01fe39c/scripts/addr2line.py -o /opt/scripts/addr2line.py &&
    curl https://raw.githubusercontent.com/redpanda-data/seastar/2a9504b3238cba4150be59353bf8d0b3a01fe39c/scripts/seastar-addr2line -o /opt/scripts/seastar-addr2line &&
    chmod +x /opt/scripts/seastar-addr2line
}

$@
