
import pytest

from redis import Redis
from rq import Queue as QueueRQ
from testcontainers.redis import RedisContainer

from bafrapy.logger.log import LoguruLogger as log
from bafrapy.tasks.queue import Queue, SerializableTask


class TestSerializableTask(SerializableTask):
    def get_task_key(self):
        return "test"
    
    def serialize(self):
        return "test"
    
    def load(self, data):
        return "test"
    

class IntegrationQueueTestBase:
    redis_client: Redis
    queue_rq: QueueRQ

    def get_queue(self):
        return self.__class__.queue

    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self):
        log().deactivate()
        container = RedisContainer("redis:latest")
        container.with_exposed_ports(6379)
        container.start()
        self.__class__.redis_client = container.get_connection_url()
        self.__class__.queue_rq = QueueRQ("bafrapy-test", connection=self.__class__.redis_client)
        yield

        container.stop()
