##
# Copyright (c) 2023, Chad Juliano, Kinetica DB Inc.
##

from __future__ import annotations
from pydantic import BaseModel, Field

class SuggestContext(BaseModel):
    table: str | None = Field(default=None, title="Name of table")
    description: str | None = Field(default=None, title="Table description")
    columns: list[str] | None = Field(default=None, title="Table columns list")
    rules: list[str] | None = Field(default=None, title="Rules that apply to the table.")
    samples: dict | None = Field(default=None, title="Samples that apply to the entire context.")

    def to_system_str(self) -> str:
        lines = []
        lines.append(f"CREATE TABLE {self.table} AS")
        lines.append("(")

        if(not self.columns or len(self.columns) == 0):
            ValueError(detail="columns list can't be null.")

        columns = []
        for column in self.columns:
            column = column.replace("\"", "").strip()
            columns.append(f"   {column}")
        lines.append(",\n".join(columns))
        lines.append(");")

        if(self.description):
            lines.append(f"COMMENT ON TABLE {self.table} IS '{self.description}';")

        if(self.rules and len(self.rules) > 0):
            lines.append(f"-- When querying table {self.table} the following rules apply:")
            for rule in self.rules:
                lines.append(f"-- * {rule}")

        result = "\n".join(lines)
        return result
    

class SuggestPayload(BaseModel):
    question: str = None
    context: list[SuggestContext]

    def get_system_str(self) -> str:
        lines = []
        for table_context in self.context:
            if(table_context.table is None):
                continue
            context_str = table_context.to_system_str()
            lines.append(context_str)
        return "\n\n".join(lines)
    

    def get_messages(self) -> str | None:
        messages = []
        for context in self.context:
            if(context.samples is None):
                continue
            for question, answer in context.samples.items():
                # unescape double quotes
                answer = answer.replace("''", "'")
                
                messages.append(dict(role="user", content=question))
                messages.append(dict(role="assistant", content=answer))
        return messages
    
    def to_completion(self) -> str:
        messages = []
        messages.append(dict(role="system", content=self.get_system_str()))
        messages.extend(self.get_messages())
        messages.append(dict(role="user", content=self.question))
        response = dict(messages=messages)
        return response


class SuggestRequest(BaseModel):
    payload: SuggestPayload

class CompletionRequest(BaseModel):
    messages: list[dict]

# Output Types

class Message(BaseModel):
    role: str = Field(default=None, title="One of [user|assistant|system]")
    content: str

class Choice(BaseModel):
    index: int
    message: Message = Field(default=None, title="The generated SQL")
    finish_reason: str

class Usage(BaseModel):
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int

class SqlResponse(BaseModel):
    id: str
    object: str
    created: int
    model: str
    choices: list[Choice]
    usage: Usage
    prompt: str = Field(default=None, title="The input question")

class CompletionResponse(BaseModel):
    status: str
    data: SqlResponse
