import json
from collections.abc import Iterator
from typing import Union, List, Any

import requests
from langchain.schema.language_model import LanguageModelInput
from requests import Timeout
from langchain.schema.messages import SystemMessage, HumanMessage, BaseMessage, AIMessage
from danswer.configs.model_configs import GEN_AI_API_ENDPOINT
from danswer.configs.model_configs import GEN_AI_MAX_OUTPUT_TOKENS
from danswer.llm.interfaces import LLM
from danswer.llm.utils import convert_lm_input_to_basic_string
from danswer.utils.logger import setup_logger

from danswer.configs.model_configs import GEN_AI_API_KEY
from danswer.configs.model_configs import GEN_AI_MODEL_VERSION
from danswer.configs.model_configs import GEN_AI_TEMPERATURE
from danswer.server.query_and_chat.models import CreateChatMessageRequest

logger = setup_logger()


class CustomModelServer(LLM):
    """This class is to provide an example for how to use Danswer
    with any LLM, even servers with custom API definitions.
    To use with your own model server, simply implement the functions
    below to fit your model server expectation

    The implementation below works against the custom FastAPI server from the blog:
    https://medium.com/@yuhongsun96/how-to-augment-llms-with-private-data-29349bd8ae9f
    """

    @property
    def requires_api_key(self) -> bool:
        return True

    def __init__(
            self,
            # Not used here but you probably want a model server that isn't completely open
            # api_key: str | None,
            timeout: int,
            endpoint: str | None = GEN_AI_API_ENDPOINT,
            api_key: str | None = GEN_AI_API_KEY,
            model: str | None = GEN_AI_MODEL_VERSION,
            max_output_tokens: int = GEN_AI_MAX_OUTPUT_TOKENS,
            temperature: str | None = GEN_AI_TEMPERATURE
    ):
        self._api_key = api_key
        if not endpoint:
            raise ValueError(
                "Cannot point Danswer to a custom LLM server without providing the "
                "endpoint for the model server."
            )

        self._endpoint = endpoint
        self._max_output_tokens = max_output_tokens
        self._timeout = timeout
        self._model = model
        self._temperature = temperature

    # def _execute(self, request: Union[HumanMessage, List[HumanMessage]]) -> str:
    # def _execute(self, input: Union[tuple[str, Any], str, BaseMessage, HumanMessage, SystemMessage, AIMessage]) -> str:
    def _execute(self, input: Union[tuple[str, Any], str, BaseMessage, HumanMessage, SystemMessage, AIMessage]) -> str:
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self._api_key
        }
        logger.info(f"input{input}")
        # 初始化一个空列表来存储消息内容
        human_messages_content = []
        system_messages_content = []
        ai_messages_content = []
        for item in input:
            if isinstance(item, HumanMessage):
                # 如果输入是 HumanMessage 类型，直接提取内容
                human_messages_content.append(item.content)
            elif isinstance(item, AIMessage):
                ai_messages_content.append(item.content)
            elif isinstance(item, SystemMessage):
                # 如果输入是 SystemMessage 类型，不提取并直接返回，避免执行后续post等操作
                system_messages_content.append(item.content)
            else:
                # 未知类型，记录错误或抛出异常
                logger.error(f"Unsupported input type: {type(item)}")
                raise ValueError(f"Unsupported input type: {type(item)}")

        # 组合所有消息内容
        combined_message = "Human Messages: " + " | ".join(human_messages_content) + \
                           "\nSystem prompts: " + " | ".join(system_messages_content) #+ \
                           # "\nAi assistant: " + " | ".join(ai_messages_content)

        # 检查是否有任何消息要发送
        if not (human_messages_content or system_messages_content or ai_messages_content):
            logger.info("No messages to post, skipping.")
            return ""

        logger.info(f"Ready to post combined message:\n {combined_message}")

        # 构建请求数据
        data = {
            "messages": [{"role": "user", "content": combined_message}],
            "temperature": self._temperature,
            "model": self._model
        }

        try:
            response = requests.post(
                self._endpoint, headers=headers, json=data, timeout=self._timeout
            )
            logger.info("post请求发送成功")
        except Timeout as error:
            raise Timeout(f"Model inference to {self._endpoint} timed out") from error

        response.raise_for_status()
        # 假设 response.content 是模型返回的JSON字符串
        response_json = json.loads(response.content)

        # 访问第一个 choice 的内容
        if response_json.get("choices"):
            generated_text = response_json["choices"][0]["message"]["content"]
            logger.info(f"Generated text: {generated_text}")
        else:
            generated_text = ""
            logger.info("No generated text found.")
        return generated_text

    def log_model_configs(self) -> None:
        logger.debug(f"Custom model at: {self._endpoint}")

    def invoke(self, prompt: LanguageModelInput) -> str:
        return self._execute(prompt)

    def stream(self, prompt: LanguageModelInput) -> Iterator[str]:
        yield self._execute(prompt)
