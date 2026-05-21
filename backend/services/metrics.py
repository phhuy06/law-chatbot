"""Prometheus metrics for the chatbot — exposed at /metrics."""
from prometheus_client import Counter, Histogram

# Counter labeled by which pipeline path served the response.
# Values: "smalltalk" | "cache" | "rag" | "no_info" | "threshold_refused" | "error"
chat_requests_total = Counter(
    "chat_requests_total",
    "Total /api/chat requests, labeled by which path served them",
    ["path"],
)

# Latency histogram, labeled by path so smalltalk's <10ms is separable
# from full-RAG's 1-3s.
chat_latency_seconds = Histogram(
    "chat_latency_seconds",
    "End-to-end latency of /api/chat requests in seconds",
    ["path"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0),
)

# Distribution of how many source documents we returned per RAG answer.
rag_sources_returned = Histogram(
    "rag_sources_returned",
    "Number of source documents returned per RAG answer",
    buckets=(0, 1, 2, 3, 5, 10),
)
