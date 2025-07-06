#!/usr/bin/env bash
set -euo pipefail

IMAGE_TAG=${1:-mysqljsonparse:alpine-static}
ARTIFACT_BASE=${2:-libmysqljsonparse.so}

if ! docker buildx version >/dev/null 2>&1; then
    echo "[ERROR] Docker buildx is not available. Please install Docker Desktop or enable buildx." >&2
    exit 1
fi

BUILDER_NAME="multiarch-builder"
if ! docker buildx inspect "$BUILDER_NAME" >/dev/null 2>&1; then
    echo "[INFO] Creating buildx builder '$BUILDER_NAME'..."
    docker buildx create --name "$BUILDER_NAME" --driver docker-container --use
fi

docker buildx use "$BUILDER_NAME"

extract_artifact() {
    local platform=$1
    local artifact_name=$2
    local platform_tag="${IMAGE_TAG}-${platform//\//-}"
    
    echo "[INFO] Building single-platform image for $platform..."
    docker buildx build --platform "$platform" -t "$platform_tag" --load .
    
    echo "[INFO] Creating temporary container from $platform image..."
    local cid=$(docker create "$platform_tag")
    trap "docker rm -fv '$cid' >/dev/null" EXIT
    
    echo "[INFO] Copying '$artifact_name' from $platform container to host..."
    if docker cp "${cid}:/${ARTIFACT_BASE}" "./${artifact_name}"; then
        echo "[SUCCESS] Artifact '$artifact_name' extracted to $(pwd)"
    else
        echo "[ERROR] Failed to find '${ARTIFACT_BASE}' inside the $platform image." >&2
        return 1
    fi
}

# Extract ARM64 artifact
extract_artifact "linux/arm64" "libmysqljsonparse.so"

# Extract AMD64 artifact  
extract_artifact "linux/amd64" "libmysqljsonparse_x86_64.so"

echo "[SUCCESS] Both artifacts built successfully:"
echo "  - libmysqljsonparse.so (ARM64)"
echo "  - libmysqljsonparse_x86_64.so (AMD64)" 