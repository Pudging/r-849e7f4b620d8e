FROM python:3.12-slim-bookworm

RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipeline.py identity.py clean.sql session.sql output.sql /app/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    OUTPUT_DIR=/output \
    SCRATCH_DIR=/scratch \
    TMPDIR=/scratch \
    HOME=/scratch \
    NCPU=8 \
    MEM_GB=24
WORKDIR /app
USER app
CMD ["python3", "/app/pipeline.py"]
