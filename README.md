# Distributed Job & Task Scheduling Engine

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An enterprise-grade, distributed background job scheduling and execution engine built with Python and FastAPI. It provides guaranteed task execution, distributed locking across multi-node worker pools, exponential backoff retries, cron scheduling, and a real-time web monitoring dashboard.

---

## 🌟 Key Features

1. **Task Management API**:
   - Register one-time immediate jobs, future scheduled timestamps, or recurring cron expressions (`* * * * *`, `@every 10s`, `@hourly`, `@daily`).
   - Interactive Swagger API docs (`/docs`) and OpenAPI spec.
2. **Distributed Worker Pool & Concurrency**:
   - Thread pool concurrency per worker node.
   - Non-blocking polling with atomic leases preventing duplicate executions across cluster instances.
3. **Distributed Locking & Worker Crash Recovery**:
   - Time-To-Live (TTL) lease locks with heartbeat extension.
   - **Zombie Task Recovery**: If a worker node crashes mid-execution (segfault, power loss, OOM kill), surviving workers automatically detect expired locks and safely re-queue or execute the task.
4. **Fault Tolerance & Exponential Backoff Retries**:
   - Configurable retry limits (`max_retries`) and base delay per task.
   - Automatically applies exponential backoff: `delay = base * (2 ^ (attempt - 1)) + jitter`.
   - Transitions to a **Dead-Letter Queue (`FAILED`)** once retry thresholds are exhausted, preserving full error traces.
5. **Full Cron & Interval Engine**:
   - Standard 5-field cron parsing (`minute hour dom month dow`).
   - Interval syntaxes such as `@every 15s`, `@every 5m`, `@daily`.
   - Automatic calculation and scheduling of subsequent run cycles.
6. **Real-Time Web Monitoring Dashboard**:
   - Built-in single-page web UI at `http://127.0.0.1:8000/`.
   - Live metrics counters (Pending, Running, Success, Retrying, Dead-Letter).
   - Quick demo task launchpad (emails, reports, flaky simulated network services).
   - Detailed task audit logs & execution trace modal.
   - Active worker node cluster and heartbeat monitor.

---

## 📐 Architecture & State Machine

```
                      +-----------------------------+
                      |   Client / Web Dashboard    |
                      +--------------+--------------+
                                     |
                                     v
                        [ FastAPI REST Gateway ]
                                     |
                                     v
                       +---------------------------+
                       |    SQLite (WAL Mode) /    |
                       |    Shared Task Queue      |
                       +-------------+-------------+
                                     |
           +-------------------------+-------------------------+
           |                                                   |
           v                                                   v
+-----------------------+                           +-----------------------+
|  Worker Node 1        |                           |  Worker Node 2        |
| - Polling Thread      |                           | - Polling Thread      |
| - Atomic Lock Lease   | <--- Distributed Lock --->| - Atomic Lock Lease   |
| - Thread Pool (N=4)   |                           | - Thread Pool (N=4)   |
| - Heartbeat Daemon    |                           | - Heartbeat Daemon    |
+-----------------------+                           +-----------------------+
```

### Task Lifecycle State Machine

```
                     +-------------------+
                     |      PENDING      | <----+
                     +---------+---------+      | (Recurring Cron Rescheduled)
                               |                |
                       (Atomic Worker Lease)    |
                               v                |
                     +-------------------+      |
                     |      RUNNING      |      |
                     +----+---------+----+      |
                          |         |           |
            (Task Fails)  |         | (Success) |
                          v         +-----------> [ SUCCESS ]
             +--------------------+
             |  Attempt < Max?    |
             +----+----------+----+
             Yes  |          | No
                  v          v
            +-----------+  +-------------+
            |  RETRYING |  |   FAILED    | (Dead-Letter Queue)
            +-----+-----+  +------+------+
                  |               |
       (Exponential Backoff)      | (Manual API Retry)
                  +---------------+
```

---

## 🚀 Quick Start

### 1. Requirements
- Python 3.10+
- `fastapi`, `uvicorn`, `pydantic`

### 2. Run the Engine & Web UI
To start both the API server and the embedded worker engine in a single command:

```powershell
python run.py
```

Output:
```
========================================================================
    DISTRIBUTED JOB & TASK SCHEDULING ENGINE (v1.0.0)
========================================================================
 Mode:             FULL CLUSTER (API SERVER + EMBEDDED WORKER POOL)
 Worker Capacity:  4 concurrent threads
 Web Dashboard:    http://127.0.0.1:8000/
 Interactive API:  http://127.0.0.1:8000/docs
 Health Endpoint:  http://127.0.0.1:8000/api/health
========================================================================
```

Open `http://127.0.0.1:8000/` in your browser to interact with the dashboard!

---

## 🧪 Running Automated Unit & Integration Tests

The project includes unit and integration tests covering cron parsing, distributed locking mutual exclusion, concurrency, exponential backoff, dead-letter transitions, and worker crash recovery:

```powershell
python -m unittest tests/test_scheduler.py
```

Test Results:
```
Ran 8 tests in 15.375s
OK
```

---

## 🐳 Docker & Multi-Node Cluster Execution

You can run a complete multi-node distributed cluster using Docker Compose:

```bash
docker compose up --build
```

This spins up:
- `scheduler-api`: Primary HTTP REST API, Web UI, and Worker Node 1.
- `scheduler-worker-2`: Secondary distributed worker node sharing the database and competing for tasks via distributed locks.

---

## 📡 API Reference

### 1. Register a Task
`POST /api/tasks`

**Immediate Task Payload:**
```json
{
  "name": "send_email",
  "payload": {
    "to": "user@example.com",
    "subject": "System Alert"
  },
  "schedule_type": "IMMEDIATE",
  "max_retries": 3,
  "retry_delay_seconds": 2.0
}
```

**Recurring Cron Payload:**
```json
{
  "name": "database_backup",
  "payload": {
    "database": "production"
  },
  "schedule_type": "CRON",
  "cron_expression": "@every 30s"
}
```

### 2. List Tasks
`GET /api/tasks?status=RUNNING`

### 3. Get Task Details & Execution Logs
`GET /api/tasks/{task_id}`

### 4. Trigger Task Immediately
`POST /api/tasks/{task_id}/trigger`

### 5. Manually Retry Dead-Letter Task
`POST /api/tasks/{task_id}/retry`

### 6. Cancel Task
`POST /api/tasks/{task_id}/cancel`

### 7. Cluster Health & Metrics
`GET /api/metrics`
`GET /api/health`

---

## 📁 Project Structure

```
project/
├── app/
│   ├── __init__.py
│   ├── config.py              # Environment variables & cluster configuration
│   ├── cron_parser.py         # 5-field Cron and interval calculator
│   ├── models.py              # Pydantic schemas & state enums
│   ├── database.py            # SQLite WAL connection manager & queries
│   ├── lock_manager.py        # Distributed locking & crash recovery engine
│   ├── task_registry.py       # Registered background job handlers
│   ├── engine.py              # Worker pool, polling loop & exponential backoff
│   ├── api.py                 # FastAPI endpoints & lifespan manager
│   └── static/
│       └── index.html         # Real-time Web Monitoring Dashboard
├── tests/
│   ├── __init__.py
│   └── test_scheduler.py      # Full test suite (8 integration tests)
├── Dockerfile                 # Production container image
├── docker-compose.yml         # Multi-node cluster orchestration
├── requirements.txt           # Python dependencies
├── run.py                     # CLI entrypoint with preset seeding
└── README.md                  # System architecture documentation
```

---

## License

This project is licensed under the MIT License.

---

## Contributing

Pull requests and feature ideas are welcome. For major changes, please open an issue first to discuss the proposed update.
