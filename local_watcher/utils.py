import sqlite3
from llama_parse import LlamaParse
from llama_index.core import SimpleDirectoryReader
import asyncio
from typing import List, Any
from llama_index.core.schema import Document
import logging
from queue import Queue
import aiofiles
import os


def init_db(db_path: str):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_events (
            id INTEGER PRIMARY KEY,
            event_type TEXT,
            file_path TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS task_queue (
            id INTEGER PRIMARY KEY,
            file_path TEXT,
            status TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS api_responses (
            id INTEGER PRIMARY KEY,
            file_path TEXT,
            response_code INTEGER,
            response_body TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )''')


async def save_docs_as_md(all_docs: List[Document], output_dir: str):
    """
    将所有文档内容保存为.md格式的文件。

    Args:
        all_docs (List[str]): 文档内容列表。
        output_dir (str): 输出文件夹路径。
    """
    for idx, doc in enumerate(all_docs):
        base_name = doc.metadata['file_name']
        file_name_without_ext = os.path.splitext(base_name)[0]
        file_path = os.path.join(output_dir, f"{file_name_without_ext}.md")
        # 构建元数据字符串
        metadata = f'#DANSWER_METADATA={{"link": "http://localhost:8000/{base_name}", "file_display_name": {base_name}}}\n'

        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            await f.write(metadata)  # 写入元数据在开头
            await f.write(doc.text)
            logging.info(f"Document {base_name} has been saved as {file_name_without_ext}.md")


async def load_and_parse_documents(input_files: List[str]) -> List[Document]:
    """
    使用LlamaParse解析指定目录中的.pdf文件，并将结果保存在output_dir目录中。

    Args:
        input_files (List[str]): 文件路径列表。
    """
    # 初始化LlamaParse解析器
    parser = LlamaParse(
        api_key="llx-h3rdXDeyWXc9eIutH0b6rGlO3mTNsEH9ZwC3kAY29K6DBLEe",
        result_type="markdown",  # "markdown"和"text" 是可用的选项
        num_workers=4,  # 如果传递了多个文件，则分为`num_workers`个API调用
        verbose=True,
        language="en"  # 可选地，你可以定义一个语言，默认为en
    )

    # 定义文件提取器
    file_extractor = {".pdf": parser}

    # 初始化SimpleDirectoryReader，并指定文件提取器
    reader = SimpleDirectoryReader(
        input_files=input_files,  # 指定输入文件
        file_extractor=file_extractor,
        filename_as_id=True
    )

    # 加载并解析数据
    documents = reader.load_data()
    return documents


def batch_get_from_queue(event_queue: Queue, batch_size: int, timeout: int = 10) -> List[str]:
    """
    从队列中批量取出元素。

    Args:
        event_queue (asyncio.Queue): 事件队列。
        batch_size (int): 每个批次的大小。
        timeout (int): 超时时间，以秒为单位。

    Returns:
        List[str]: 批量取出的路径列表。
    """
    batch = []
    for _ in range(batch_size):
        try:
            # 注意：asyncio.Queue没有直接的timeout参数，这里使用asyncio.wait_for来实现超时逻辑
            item = event_queue.get()
            batch.append(item)
            event_queue.task_done()  # 标记队列任务完成
        except asyncio.QueueEmpty:
            # 如果队列为空，则跳出循环
            break
        except asyncio.TimeoutError:
            # 如果等待超时，也跳出循环
            break
    return batch


if __name__ == "__main__":
    init_db("monitoring.db")
