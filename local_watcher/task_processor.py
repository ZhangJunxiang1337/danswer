import sqlite3
import requests
from queue import Queue, Empty
from time import sleep
import logging
import asyncio
from local_watcher.utils import batch_get_from_queue, load_and_parse_documents, save_docs_as_md


def log_task(db_path, file_path, status):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO task_queue (file_path, status) VALUES (?, ?)', (file_path, status))
        conn.commit()


def log_response(db_path, file_path, response_code, response_body):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO api_responses (file_path, response_code, response_body) VALUES (?, ?, ?)',
                       (file_path, response_code, response_body))
        conn.commit()


async def process_task(event_queue: Queue, db_path: str, output_path: str, api_endpoint: str, retry_limit: int = 3):
    while True:
        try:
            file_path_batch = batch_get_from_queue(event_queue, batch_size=4, timeout=1)
            if file_path_batch:
                logging.info("detected file path, now parsing...")
                documents = await load_and_parse_documents(file_path_batch)
                if documents:
                    await save_docs_as_md(documents, output_path)
            # log_task(db_path, file_path, "STARTED")

            for attempt in range(retry_limit):
                try:
                    response = requests.get(api_endpoint)
                    if response.status_code == 200:
                        logging.info(f"Response code: {response.status_code}, file path:")
                        log_response(db_path, "file_path", response.status_code, response.text)
                        log_task(db_path, "file_path", "SUCCESS")
                        break
                    else:
                        log_task(db_path, "file_path", f"FAILED_ATTEMPT_{attempt + 1}")
                except requests.RequestException as e:
                    log_task(db_path, "file_path", f"ERROR_ATTEMPT_{attempt + 1}")
                sleep(2)  # Wait before retrying
        except Empty:
            continue
