import sqlite3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from queue import Queue
import threading
import time
import asyncio


class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, event_queue: asyncio.Queue, db_path: str):
        super().__init__()
        self.event_queue = event_queue
        self.db_path = db_path
        self._event_cache = {}
        self._lock = threading.Lock()

    def log_event(self, event_type, file_path):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO file_events (event_type, file_path) VALUES (?, ?)', (event_type, file_path))
            conn.commit()

    def should_ignore(self, file_path, event_type):
        ignored_files = ['.DS_Store']
        ignored_patterns = ['sb-', '.swp', '.tmp']
        if event_type == 'deleted':  # 直接忽略删除事件
            return True
        for pattern in ignored_patterns:
            if pattern in file_path:
                return True
        for name in ignored_files:
            if file_path.endswith(name):
                return True
        return False

    def on_any_event(self, event):
        if not event.is_directory and not self.should_ignore(event.src_path, event.event_type):
            with self._lock:
                self._event_cache[event.src_path] = event
                threading.Timer(0.5, self.process_event, [event.src_path]).start()

    def process_event(self, file_path):
        with self._lock:
            if file_path in self._event_cache:
                event = self._event_cache.pop(file_path)
                self.log_event(event.event_type, event.src_path)
                self.event_queue.put(event.src_path)  # Put the file path into the queue


def start_monitoring(path: str, event_queue: Queue, db_path: str):
    event_handler = FileChangeHandler(event_queue, db_path)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    return observer
