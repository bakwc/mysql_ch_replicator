# syntax = docker/dockerfile:1.4

# Use the specified Python base image
FROM python:3.12.4-slim-bookworm

USER root

# Set environment variables to ensure non-interactive installation
ENV DEBIAN_FRONTEND=noninteractive \
    PYPY3_VERSION=7.3.17 \
    PYPY3_PYTHON_VERSION=3.10

# Install necessary packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        bzip2 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Detect architecture and set the appropriate download URL
ARG TARGETARCH
ENV PYPY3_DOWNLOAD_URL=""

RUN --mount=type=cache,target=/tmp \
    case "$TARGETARCH" in \
        "amd64") \
            echo "Building for amd64 architecture." && \
            ARCH_SUFFIX="linux64" && \
            PYPY3_DOWNLOAD_URL="https://downloads.python.org/pypy/pypy3.10-v${PYPY3_VERSION}-${ARCH_SUFFIX}.tar.bz2" ;; \
        "arm64") \
            echo "Building for arm64 architecture." && \
            ARCH_SUFFIX="aarch64" && \
            PYPY3_DOWNLOAD_URL="https://downloads.python.org/pypy/pypy3.10-v${PYPY3_VERSION}-${ARCH_SUFFIX}.tar.bz2" ;; \
        *) \
            echo "Architecture $TARGETARCH not supported." && \
            exit 1 ;; \
    esac && \
    echo "Downloading PyPy3 from $PYPY3_DOWNLOAD_URL" && \
    wget "$PYPY3_DOWNLOAD_URL" -O /tmp/pypy3.tar.bz2 && \
    mkdir -p /opt/pypy3 && \
    tar -xjf /tmp/pypy3.tar.bz2 -C /opt/pypy3 --strip-components=1 && \
    rm /tmp/pypy3.tar.bz2

# Create symbolic links for PyPy3 and PyPy
RUN ln -sf /opt/pypy3/bin/pypy3 /usr/local/bin/pypy3 && \
    ln -sf /opt/pypy3/bin/pypy3 /usr/local/bin/pypy

# Download and install pip for PyPy3 using get-pip.py
RUN wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py && \
    pypy3 /tmp/get-pip.py && \
    rm /tmp/get-pip.py

# Create a symbolic link for PyPy3's pip as pip-pypy
RUN ln -sf /opt/pypy3/bin/pip /usr/local/bin/pip-pypy

# Upgrade pip, setuptools, and wheel for PyPy3
RUN pip-pypy install --no-cache --upgrade pip setuptools wheel

# (Optional) Verify installations
RUN python3 --version && \
    pypy --version && \
    pip --version && \
    pip-pypy --version

# Clean up unnecessary packages to reduce image size
RUN apt-get update && \
    apt-get remove --purge -y wget && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /app/
COPY requirements.txt /app/requirements.txt
COPY requirements-dev.txt /app/requirements-dev.txt
COPY requirements-pypy.txt /app/requirements-pypy.txt

RUN pip install -r /app/requirements.txt
RUN pip install -r /app/requirements-dev.txt
RUN pip-pypy install -r /app/requirements-pypy.txt


# Set the default command to bash (optional)
CMD ["bash"]
