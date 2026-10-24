# The `spark` service: Java 17 and the job's own environment, so that the container runs the very
# ingestio, pyspark and psycopg of uv.lock. pyspark brings Spark; the first build compiles its
# source archive of about 450 MB and takes minutes.
FROM eclipse-temurin:17.0.20.1_1-jre-noble

COPY --from=ghcr.io/astral-sh/uv:0.9.30 /uv /usr/local/bin/uv

ENV UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    PATH=/opt/venv/bin:$PATH \
    HOME=/home/spark

# uid and gid 1000, so that the files written under /data belong on the host to the usual first
# user. The base image has a user "ubuntu" with that id: it goes. The home holds Spark's Ivy cache
# of the pgJDBC driver.
RUN (userdel -r ubuntu || true) \
    && groupadd --gid 1000 spark \
    && useradd --uid 1000 --gid 1000 --create-home --home-dir /home/spark spark

WORKDIR /app
COPY .python-version pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project

# The job's code, as the repository lays it out: the job in src/, the preparation script in
# scripts/. They run as `python src/main.py` and `python scripts/prepare_landing.py` from /app.
COPY src/ ./src/
COPY scripts/ ./scripts/
RUN chown -R 1000:1000 /app
