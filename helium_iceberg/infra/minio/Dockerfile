FROM minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1
RUN groupadd -r minio && useradd -r -g minio minio \
    && mkdir -p /data && chown minio:minio /data
USER minio
CMD ["minio", "server", "/data", "--console-address", ":9001"]
