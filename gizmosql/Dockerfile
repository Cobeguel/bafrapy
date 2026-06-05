FROM ghcr.io/gizmodata/gizmosql:v1.28.0-slim

USER root

RUN mkdir -p /opt/gizmosql/init /opt/gizmosql/data && \
    chown -R app_user:app_user /opt/gizmosql/init /opt/gizmosql/data

COPY init/init_ducklake.sql /opt/gizmosql/init/init_ducklake.sql

RUN chown app_user:app_user /opt/gizmosql/init/init_ducklake.sql

USER app_user