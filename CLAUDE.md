# CLAUDE.md

## Project Overview

Vietnamese Legal Chatbot - RAG-based Q&A system for Vietnamese law documents.
Lambda architecture: Spark Streaming (speed) + Spark Batch + Elasticsearch serving + GPT-4o mini.

## Tech Stack

- **Crawler**: Scrapy + Playwright (thuvienphapluat.vn)
- **Message Queue**: Apache Kafka
- **Storage**: MinIO (S3-compatible)
- **Processing**: PySpark (batch + structured streaming)
- **Search/Vector DB**: Elasticsearch 8.x (full-text + kNN HNSW)
- **Cache**: Redis
- **Backend**: FastAPI (Python)
- **Frontend**: React + TypeScript (Vite)
- **LLM**: GPT-4o mini (OpenAI API)
- **Orchestration**: Kubernetes (Minikube for dev/demo)

## Development Workflow

- `bash k8s/start.sh` to deploy infra + app to minikube
- `source .venv/bin/activate` for Python work
- `cd frontend && npm run dev` for frontend

## Git Rules

- **Commit after every feature**: Each completed feature, module, or logical unit of work MUST be committed immediately with `git add` + `git commit` before moving to the next task.
- Do NOT batch multiple features into a single commit.
- Commit message format: `<type>: <short description>`
  - Types: `feat`, `fix`, `refactor`, `docs`, `chore`, `test`
  - Example: `feat: add kafka producer for crawled documents`
- Always stage specific files, not `git add -A`.
- **NEVER add Co-Authored-By or any Claude/AI co-author lines to commits.**
