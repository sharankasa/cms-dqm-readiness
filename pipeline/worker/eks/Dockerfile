# EKS worker image (the pilot-the readiness inputs): CMS's reference engine (fqm-execution, unmodified, pinned by package-lock)
# + the S1 runner + the S3 wrapper (AWS SDK, pinned by its own package-lock). No data, no value sets,
# no credentials inside: inputs come from S3 at run time, credentials from EKS Pod Identity.
# Build from the project root for the nodes' CPU type (x86_64), e.g.:
#   docker buildx build --platform linux/amd64 -t <registry>/dqm-worker:<tag> -f pipeline/worker/eks/Dockerfile --push .
FROM node:22-slim
# python3 (standard library only) for V, the independent implementation (pipeline/v)
RUN apt-get update && apt-get install -y --no-install-recommends python3 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY testcases/package.json testcases/package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force
COPY pipeline/worker/eks/package.json pipeline/worker/eks/package-lock.json /app/s3/
RUN cd /app/s3 && npm ci --omit=dev --no-audit --no-fund && npm cache clean --force
COPY pipeline/s1/run_engine.js /app/run_engine.js
COPY pipeline/worker/eks/worker_s3.js pipeline/worker/eks/profile_run.js pipeline/worker/eks/profile_summary.js /app/s3/
COPY pipeline/v/reference_measures.py pipeline/v/compare_v.py /app/v/
RUN mkdir -p /work && chown node:node /work
USER node
ENV WORK_DIR=/work RUN_ENGINE=/app/run_engine.js V_DIR=/app/v
ENTRYPOINT ["node", "/app/s3/worker_s3.js"]
