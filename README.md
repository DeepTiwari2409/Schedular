# Distributed Task Scheduler

A distributed, fault-tolerant task execution system built using Python, Flask, PostgreSQL, and Redis. The system is designed to reliably schedule and execute asynchronous jobs across multiple workers while ensuring correctness under failures.

---

## Overview

This project implements a production-style distributed scheduler that:

- Supports concurrent job execution across multiple workers  
- Ensures fault tolerance using lease-based ownership  
- Provides at-least-once execution guarantees  
- Handles worker crashes, retries, and rebalancing  
- Scales horizontally with multiple schedulers and workers  

The design mirrors real-world backend systems used for large-scale job orchestration.

---

## Design Goals

### Distributed Scheduling
- Multiple schedulers operate concurrently without conflicts  
- PostgreSQL row-level locking (`FOR UPDATE SKIP LOCKED`) ensures safe job acquisition  
- No single point of failure  

### Lease-Based Ownership
- Each job is assigned with a time-bound lease  
- Workers must periodically renew the lease via heartbeats  
- Expired leases allow reassignment of jobs  

### Fencing Tokens
- Each job execution uses a monotonically increasing version  
- Prevents stale workers from overwriting results  
- Ensures correctness during retries and failures  

### Execution Semantics
- At-least-once execution guarantee  
- Jobs are retried on failure  
- Idempotency is expected at the application level  

---

## Architecture

```
Client/API → Flask API → PostgreSQL (Job Store)
                         ↓
                Schedulers (multiple)
                         ↓
                     Workers
```

### Components

- Flask API: Handles job submission and status queries  
- PostgreSQL: Persistent job store and concurrency control  
- Schedulers: Poll and assign jobs using database locking  
- Workers: Execute jobs and maintain leases  

---

## Job Lifecycle

1. Job is submitted via API and stored in PostgreSQL  

2. Scheduler fetches jobs using:
   ```sql
   SELECT * FROM jobs
   WHERE status = 'PENDING'
   FOR UPDATE SKIP LOCKED;
   ```

3. Job is marked as RUNNING and assigned to a worker  

4. Worker:
   - Executes the job  
   - Sends periodic heartbeats  

5. Outcomes:
   - Success → marked COMPLETED  
   - Failure → retried with backoff  
   - Worker crash → lease expires → job reassigned  

---

## Features

- Job submission and tracking  
- Concurrent scheduling with no duplicate assignment  
- Lease-based execution model  
- Automatic retries with backoff  
- Heartbeat mechanism for failure detection  
- Self-healing via lease expiry and reassignment  

---

## Correctness Guarantees

- No duplicate ownership: ensured by row-level locking  
- No stale writes: enforced via fencing tokens  
- No job loss: guaranteed by retry mechanism  

---

## Database Schema (Simplified)

```sql
CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    status TEXT,
    payload JSONB,
    attempts INT,
    max_attempts INT,
    lease_expiry TIMESTAMP,
    version INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

---

## API Endpoints

### Create Job
```
POST /jobs
```

### Get Job Status
```
GET /jobs/{job_id}
```

### List Jobs
```
GET /jobs
```

---

## Running Locally

### Clone the repository
```bash
git clone https://github.com/DeepTiwari2409/Schedular.git
cd Schedular
```

### Start services
```bash
docker-compose up --build
```

### Access API
```
http://localhost:5000
```
