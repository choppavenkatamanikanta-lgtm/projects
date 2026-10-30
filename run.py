import argparse
import logging
import os
import sys
import time
from datetime import datetime

import uvicorn

from app.config import (
    API_HOST,
    API_PORT,
    DEFAULT_WORKER_CONCURRENCY,
)
from app.cron_parser import get_next_run
from app.database import create_task, get_metrics, init_db
from app.engine import SchedulerEngine
from app.models import ScheduleType, TaskCreateRequest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scheduler.main")


def seed_demo_tasks():
    """Seeds rich sample tasks so the engine and dashboard display active operations immediately."""
    metrics = get_metrics()
    if metrics["total_tasks"] > 0:
        return

    logger.info("Initializing sample background tasks...")

    # 1. Immediate email dispatch
    create_task(
        "task-email-demo",
        TaskCreateRequest(
            name="send_email",
            payload={"to": "dev-team@cloudcorp.io", "subject": "Engine cluster online", "delay": 0.5},
            schedule_type=ScheduleType.IMMEDIATE,
        ),
        scheduled_at=datetime.now(),
    )

    # 2. Billing Report Generation
    create_task(
        "task-report-demo",
        TaskCreateRequest(
            name="generate_report",
            payload={"report_type": "monthly_billing", "account_id": "ACC-9921", "delay": 0.8},
            schedule_type=ScheduleType.IMMEDIATE,
        ),
        scheduled_at=datetime.now(),
    )

    # 3. Flaky Service (demonstrates exponential backoff retry)
    create_task(
        "task-flaky-demo",
        TaskCreateRequest(
            name="flaky_service",
            payload={"sim_key": "flaky_demo_key", "fail_until_attempt": 3},
            schedule_type=ScheduleType.IMMEDIATE,
            max_retries=3,
            retry_delay_seconds=2.0,
        ),
        scheduled_at=datetime.now(),
    )

    # 4. Recurring Cron Job (@every 15s)
    cron_expr = "@every 15s"
    create_task(
        "task-cron-demo",
        TaskCreateRequest(
            name="database_backup",
            payload={"database": "production_primary", "delay": 0.6},
            schedule_type=ScheduleType.CRON,
            cron_expression=cron_expr,
        ),
        scheduled_at=get_next_run(cron_expr),
    )

    logger.info("Sample background tasks seeded successfully.")


def print_banner(host: str, port: int, concurrency: int, mode: str):
    banner = f"""
========================================================================
    DISTRIBUTED JOB & TASK SCHEDULING ENGINE (v1.0.0)
========================================================================
 Mode:             {mode}
 Worker Capacity:  {concurrency} concurrent threads
 Web Dashboard:    http://{host}:{port}/
 Interactive API:  http://{host}:{port}/docs
 Health Endpoint:  http://{host}:{port}/api/health
========================================================================
  * Press Ctrl+C to stop the engine and server.
========================================================================
"""
    print(banner, flush=True)


def main():
    parser = argparse.ArgumentParser(description="Distributed Job & Task Scheduling Engine")
    parser.add_argument("--host", default=API_HOST, help="API server host")
    parser.add_argument("--port", type=int, default=API_PORT, help="API server port")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_WORKER_CONCURRENCY, help="Worker concurrency")
    parser.add_argument("--worker-only", action="store_true", help="Run dedicated worker without HTTP server")
    parser.add_argument("--seed", action="store_true", default=True, help="Seed sample demo tasks")

    args = parser.parse_args()

    init_db()

    if args.seed:
        seed_demo_tasks()

    if args.worker_only:
        print_banner(args.host, args.port, args.concurrency, mode="STANDALONE WORKER NODE")
        engine = SchedulerEngine(concurrency=args.concurrency)
        engine.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Shutting down worker...")
            engine.stop(wait=True)
            sys.exit(0)
    else:
        # Full mode: REST API server + Embedded worker engine
        os.environ["WORKER_CONCURRENCY"] = str(args.concurrency)
        os.environ["START_EMBEDDED_WORKER"] = "true"
        print_banner(args.host, args.port, args.concurrency, mode="FULL CLUSTER (API SERVER + EMBEDDED WORKER POOL)")
        uvicorn.run(
            "app.api:app",
            host=args.host,
            port=args.port,
            log_level="info",
            access_log=False,
        )


if __name__ == "__main__":
    main()
