##
# Copyright (c) 2024, Chad Juliano, Kinetica DB Inc.
##

from typing import Any, List, Dict, Mapping, Optional, cast
from pathlib import Path
from importlib.metadata import version
import json
import re

from langchain_core.pydantic_v1 import Field, root_validator
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
)

from gpudb import GPUdb
from kinetica.sa_dto import SuggestRequest, CompletionResponse, SqlResponse
from kinetica.sa_datafile import SaDatafile


class KineticaChatLLM(BaseChatModel):

    kdbc: GPUdb
    """ Kinetica DB connection. """

    @classmethod
    def _create_kdbc(cls, host: str, login: str, password: str) -> GPUdb:
        options = GPUdb.Options()
        options.username = login
        options.password = password
        options.skip_ssl_cert_verification = True
        options.disable_failover = True
        options.logging_level = 'INFO'
        kdbc = GPUdb(host=host, options = options)
        return kdbc

    @root_validator()
    def validate_environment(cls, values: Dict) -> Dict:
        kdbc = values['kdbc']
        print(f"Connected to Kinetica: {kdbc.get_url()}. (api={version('gpudb')}, server={kdbc.server_version})")
        return values
    
    @property
    def _llm_type(self) -> str:
        return "kinetica-sqlassist"

    @property
    def _identifying_params(self) -> Mapping[str, Any]:
        """Get the identifying parameters."""
        return dict(kinetica_version=str(self.kdbc.server_version), 
                    api_version=version('gpudb'))

    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> ChatResult:
        if stop is not None:
            raise ValueError("stop kwargs are not permitted.")

        dict_messages = [self._convert_message_to_dict(m) for m in messages]
        sql_response = self._submit_completion(dict_messages)
        generated_dict = sql_response.choices[0].message.model_dump()
        generated_message = self._convert_message_from_dict(generated_dict)

        llm_output = dict(
            input_tokens = sql_response.usage.prompt_tokens, 
            output_tokens = sql_response.usage.completion_tokens,
            model_name = sql_response.model)
        return ChatResult(generations=[ChatGeneration(message=generated_message)], llm_output=llm_output)

    def load_messages_from_context(self, context_name: str) -> List[BaseMessage]:
        # query kinetica for the prompt
        sql = f"GENERATE PROMPT WITH OPTIONS (CONTEXT_NAMES = '{context_name}')"
        result = self._execute_sql(sql)
        prompt = result['Prompt']
        prompt_json = json.loads(prompt)

        # convert the prompt to messages
        request = SuggestRequest.model_validate(prompt_json)
        payload = request.payload

        dict_messages=[]
        dict_messages.append(dict(role="system", content=payload.get_system_str()))
        dict_messages.extend(payload.get_messages())
        messages = [self._convert_message_from_dict(m) for m in dict_messages]
        return messages
    
    def _submit_completion(self, messages: Dict) -> SqlResponse:
        request = dict(messages=messages)
        request_json = json.dumps(request)
        response_raw = self.kdbc._GPUdb__submit_request_json( '/chat/completions', request_json)
        response_json = json.loads(response_raw)

        status = response_json['status']
        if(status != "OK"):
            message = response_json['message']
            match_resp = re.compile(r'response:({.*})')
            result = match_resp.search(message)
            if(result is not None):
                response = result.group(1)
                response_json = json.loads(response)
                message = response_json['message']
            raise ValueError(message)
        
        data = response_json['data']
        response = CompletionResponse.model_validate(data)
        if(response.status != "OK"):
            raise ValueError("SQL Generation failed")
        return response.data

    def _execute_sql(self, sql: str) -> Dict:
        response = self.kdbc.execute_sql_and_decode(sql, limit=1, get_column_major=False)

        status_info = response['status_info']
        if(status_info['status'] != 'OK'):
            message = status_info['message']
            raise ValueError(message)
        
        records = response['records']
        if(len(records) != 1):
            raise ValueError("No records returned.")
        
        record = records[0]
        response_dict = {}
        for col, val in record.items():
            response_dict[col] = val
        return response_dict
    
    @classmethod
    def load_messages_from_datafile(cls, sa_datafile: Path) -> List[BaseMessage]:
        datafile_dict = SaDatafile.parse_dialogue_file(sa_datafile)
        messages = cls._convert_dict_to_messages(datafile_dict)
        return messages
    
    @classmethod
    def _convert_message_to_dict(cls, message: BaseMessage) -> Dict:
        content = cast(str, message.content)
        if isinstance(message, HumanMessage):
            role = "user"
        elif isinstance(message, AIMessage):
            role = "assistant"
        elif isinstance(message, SystemMessage):
            role = "system"
        else:
            raise ValueError(f"Got unsupported message type: {message}")
        
        message = dict(role=role, content=content)
        return message

    @classmethod
    def _convert_message_from_dict(cls, message: Dict) -> BaseMessage:
        role = message['role']
        content = message['content']
        if(role == 'user'):
            return HumanMessage(content=content)
        elif(role == 'assistant'):
            return AIMessage(content=content)
        elif(role == 'system'):
            return SystemMessage(content=content)
        else:
            raise ValueError(f"Got unsupported role: {role}") 

    @classmethod
    def _convert_dict_to_messages(cls, sa_data: Dict) -> List[BaseMessage]:
        schema = sa_data['schema']
        system = sa_data['system']
        messages = sa_data['messages']
        print(f"Importing prompt for schema: {schema}")

        result_list = []
        result_list.append(SystemMessage(content=system))
        result_list.extend([cls._convert_message_from_dict(m) for m in messages])
        return result_list
    