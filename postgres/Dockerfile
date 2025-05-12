FROM postgres:14

# Install necessary packages
RUN apt-get update && apt-get install -y \
    postgresql-14-wal2json \
    && rm -rf /var/lib/apt/lists/*

# Copy custom PostgreSQL config
COPY postgresql.conf /etc/postgresql/postgresql.conf

# Use custom postgresql.conf
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]