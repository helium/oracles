FROM minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1
RUN mkdir -p /home/minio/data && chown 1000:1000 /home/minio/data
USER 1000
CMD ["minio", "server", "/home/minio/data", "--console-address", ":9001"]
