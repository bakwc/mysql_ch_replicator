FROM alpine:3.22 AS build

RUN apk add --no-cache \
        build-base \
        clang \
        lld \
        llvm \
        cmake \
        ninja \
        musl-dev \
        binutils

WORKDIR /src

COPY . .

RUN cmake -S . -B build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_COMPILER=clang \
        -DCMAKE_CXX_COMPILER=clang++ \
        -DCMAKE_EXE_LINKER_FLAGS="-fuse-ld=lld" \
        -DCMAKE_SHARED_LINKER_FLAGS="-fuse-ld=lld" \
        && ninja -C build -v \
        && strip --strip-unneeded build/libmysqljsonparse.so \
        && echo "Library dependencies:" && ldd build/libmysqljsonparse.so

FROM scratch AS artifact

COPY --from=build /src/build/libmysqljsonparse.so /

CMD [""]