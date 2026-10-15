# Worker image: CMS's reference engine (fqm-execution, unmodified, pinned by package-lock) + the S1 runner.
# No data and no value sets inside the image: inputs and outputs are mounted (local) or read from
# S3 (EKS). Build from the project root (the .dockerignore keeps the context small):
#   docker build -t dqm-worker:dev -f pipeline/worker/Dockerfile .
FROM node:22-slim
WORKDIR /app
COPY testcases/package.json testcases/package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force
COPY pipeline/s1/run_engine.js ./run_engine.js
USER node
ENTRYPOINT ["node", "--max-old-space-size=2560", "/app/run_engine.js"]
