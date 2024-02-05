##
# Copyright (c) 2023, Chad Juliano, Kinetica DB Inc.
##

from pathlib import Path
import re
import os

class SaDatafile:

    # parse line into a dict containing role and content
    PARSER = re.compile(r"^<\|(?P<role>\w+)\|>\W*(?P<content>.*)$", re.DOTALL)

    @classmethod
    def parse_dialogue_file(cls, input_file: os.PathLike) -> dict:
        path = Path(input_file)
        schema = path.name.removesuffix('.txt')
        lines = open(input_file).read()
        return cls.parse_dialogue(lines,schema)


    @classmethod
    def parse_dialogue(cls, text: str, schema: str) -> dict:
        messages = []
        system = None

        lines = text.split('<|end|>')
        user_message = None

        for idx, line in enumerate(lines):
            line = line.strip()

            if(len(line) == 0):
                continue

            match = cls.PARSER.match(line)
            if(match is None):
                raise ValueError(f"Could not find starting token in: {line}")
            
            groupdict = match.groupdict()
            role = groupdict["role"]

            if(role == "system"):
                if(system is not None):
                    raise ValueError(f"Only one system token allowed in: {line}")
                system = groupdict['content']
            elif(role == "user"):
                if(user_message is not None):
                    raise ValueError(f"Found user token without assistant token: {line}")
                user_message = groupdict
            elif(role == "assistant"):
                if(user_message is None):
                    raise Exception(f"Found assistant token without user token: {line}")
                messages.append(user_message)
                messages.append(groupdict)
                user_message = None
            else:
                raise ValueError(f"Unknown token: {role}")

        return { "schema": schema, "system": system, "messages": messages  }
