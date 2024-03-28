import logging
from queue import Queue
from threading import Thread
from file_watcher import start_monitoring
from task_processor import process_task
from utils import init_db
import asyncio


def thread_function(event_queue: Queue, db_path: str, output_path: str, api_endpoint: str):
    # 在当前线程中创建新的事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # 将异步任务加入到事件循环中，但不立即执行
    asyncio.ensure_future(process_task(event_queue, db_path, output_path, api_endpoint))

    try:
        # 无限循环运行事件循环，直到调用 loop.stop()
        loop.run_forever()
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


async def main():
    logging.basicConfig(level=logging.INFO)
    db_path = "monitoring.db"
    init_db(db_path)

    event_queue = Queue()
    path_to_watch = "/Users/apple/Desktop/work/danswer/local_watcher/watch"  # Specify the directory to monitor
    api_endpoint = "http://10.1.11.50:3000/api/health"  # Specify the API endpoint
    output_path = "/Users/apple/Desktop/work/danswer/local_watcher/markdown"
    import os
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    # Start monitoring the directory
    observer = start_monitoring(path_to_watch, event_queue, db_path)
    # Start processing tasks from the event queue
    await process_task(event_queue, db_path, output_path, api_endpoint)

    try:
        while True:
            pass  # Keep the main thread alive
    except KeyboardInterrupt:
        # Stop the observer and join the worker thread on interrupt
        observer.stop()
        observer.join()


if __name__ == "__main__":
    asyncio.run(main())
