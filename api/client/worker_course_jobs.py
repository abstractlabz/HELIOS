# worker_course_jobs.py
import os
from redis import Redis
from rq import Queue, SimpleWorker

# Same Redis URL you already use in upgrade.py
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_conn = Redis.from_url(REDIS_URL)

def main():
    # Queue name must match the one used in upgrade.py (course_queue = Queue("course-jobs", ...))
    q = Queue("course-jobs", connection=redis_conn)

    # SimpleWorker does NOT use os.fork() -> safe on Windows
    worker = SimpleWorker([q], connection=redis_conn)

    print("=== SimpleWorker started. Listening on 'course-jobs' ===")
    worker.work(burst=False)  # burst=False -> keep running, process jobs as they come

if __name__ == "__main__":
    main()
