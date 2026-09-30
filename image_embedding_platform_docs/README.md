# Image Embedding Platform Documentation Pack

This folder contains the implementation-ready architecture package for the local-first, production-scalable image ingestion, deduplication, CNN embedding, and vector-search system.

Start with `00_START_HERE.md`.

Files:

- `00_START_HERE.md` — architecture decisions and reading order.
- `01_ARCHITECTURE.md` — end-to-end architecture, concurrency, state ownership, scaling and failures.
- `02_DEDUPLICATION.md` — SHA/pixel hash/PDQ/ORB/RANSAC/SSIM strategy.
- `03_EMBEDDINGS_AND_VECTOR_SEARCH.md` — ResNet-18 embeddings, Milvus, archive and retrieval.
- `04_SRS.md` — System Requirements Specification.
- `05_SDS.md` — Software Design Specification.
- `06_LOCAL_SETUP_FLOCI.md` — M1 Pro/16 GB local environment and Floci setup guidance.
- `07_DATASETS_AND_EVALUATION.md` — Copydays, COCO, synthetic corpus and metrics.
- `08_IMPLEMENTATION_PLAN.md` — phase-by-phase junior implementation plan.
- `09_OPERATIONS_AND_TESTING.md` — observability, load, chaos, concurrency and recovery tests.
- `10_ADRS.md` — architecture decision records.
