FROM alpine:3.24.4
RUN apk --no-cache upgrade && apk --no-cache add ca-certificates \
    && mkdir -p /lfs && chown 65532:65532 /lfs
COPY lfscache /bin/lfscache
WORKDIR /lfs
USER 65532:65532
ENTRYPOINT ["/bin/lfscache"]
