  FROM postgres:16

  # Install necessary packages
  RUN apt-get update && apt-get install -y \
      postgresql-16-postgis-3 \
      postgresql-16-postgis-3-scripts \
      postgresql-16-cron \
      git \
      make \
      gcc \
      libpq-dev \
      postgresql-server-dev-16

  # Clone and install pg_partman version 4.7.3 (for 1:1 compat with RDS)
  RUN git clone https://github.com/pgpartman/pg_partman.git \
      && cd pg_partman \
      && git checkout v4.7.3 \
      && make \
      && make install \
      && cd .. \
      && rm -rf pg_partman

  # Append custom settings to postgresql.conf
  RUN echo "shared_preload_libraries = 'postgis-3,pg_partman_bgw,pg_cron'" >> /usr/share/postgresql/postgresql.conf.sample

  # Ensure pg_partman and pg_cron are in the PostgreSQL extension directory
  RUN mkdir -p /usr/local/share/postgresql/extension \
      && ln -s /usr/share/postgresql/16/extension/pg_partman* /usr/local/share/postgresql/extension/ \
      && ln -s /usr/share/postgresql/16/extension/pg_cron* /usr/local/share/postgresql/extension/
