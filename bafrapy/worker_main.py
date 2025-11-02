# import os
# import sys

# from os import getenv

# import redis

# #from redis import Redis
# from rq import SimpleWorker

# from bafrapy.backend.taskqueues.worker import TaskFactory
# from bafrapy.tasks.backtesting import BacktetingBuilder, BacktetingPayload
# from bafrapy.tasks.syncdata import SyncDataBuilder, SyncDataPayload
# from bafrapy.tasks.syncsymbols import SyncDataBuilder, SyncDataPayload


# def main(queue_name=None):
#     if queue_name is None:
#         print("Error: Queue name must be provided as an argument")
#         print("Usage: python worker_main.py <queue_name>")
#         print("Available queues: BACKTEST_QUEUE, DATA_QUEUE, ASSETS_QUEUE")
#         sys.exit(1)

#     queue_env_var = queue_name.upper()
#     queue = getenv(queue_env_var)
    
#     if not queue:
#         print(f"Error: Queue environment variable {queue_env_var} not found")
#         sys.exit(1)
    
#     TaskFactory().register(SyncDataPayload().get_task_key(), SyncDataBuilder())
#     TaskFactory().register(BacktetingPayload().get_task_key(), BacktetingBuilder())

#     conn = redis.Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=os.getenv("REDIS_DB"))
#     worker = SimpleWorker(queue, connection=conn)

#     worker.work(with_scheduler=True)

# if __name__ == "__main__":
#     if len(sys.argv) > 1:
#         main(sys.argv[1])
#     else:
#         print("Usage: python worker_main.py <queue_env_var_name>")