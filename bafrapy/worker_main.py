import sys
from redis import Redis
from rq import Queue, SimpleWorker
from os import getenv
from bafrapy.backend.tasks.db import TaskDB
from bafrapy.backend.tasks.worker import TaskFactory
from bafrapy.backend.tasks.data import SyncDataPayload, SyncDataBuilder
from bafrapy.backend.tasks.backtesting import BacktetingPayload, BacktetingBuilder
import os
import redis

def main(queue_name=None):
    if queue_name is None:
        print("Error: Queue name must be provided as an argument")
        print("Usage: python worker_main.py <queue_name>")
        print("Available queues: BACKTEST_QUEUE, DATA_QUEUE, ASSETS_QUEUE")
        sys.exit(1)

    queue_env_var = queue_name.upper()
    queue = getenv(queue_env_var)
    
    if not queue:
        print(f"Error: Queue environment variable {queue_env_var} not found")
        sys.exit(1)
    
    TaskFactory().register(SyncDataPayload().get_task_key(), SyncDataBuilder())
    TaskFactory().register(BacktetingPayload().get_task_key(), BacktetingBuilder())

    conn = redis.Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=os.getenv("REDIS_DB"))
    worker = SimpleWorker(queue, connection=conn)
    worker.work()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python worker_main.py <queue_env_var_name>")