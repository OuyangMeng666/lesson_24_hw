import itertools
import os
import re
from dataclasses import dataclass
from itertools import islice
from typing import List, Any, Dict, Callable, Generator

import marshmallow
import marshmallow_dataclass
from flask import Flask, request
from werkzeug.exceptions import BadRequest

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def read_file(filepath: str) -> Generator:
    for row in open(filepath, 'r'):
        yield row


def perform_command(logs: Generator[Any, Any, Any], command: str, value: str) -> Any:
    if command == 'filter':
        return do_filter(logs, value)
    if command == 'map':
        return "\n".join(do_map(logs, value))
    if command == 'unique':
        return do_unique(logs)
    if command == 'sort':
        return do_sort(logs, value)
    if command == 'limit':
        return do_limit(logs, value)
    if command == 'regex':
        return do_regex(logs, value)


def do_filter(logs: Generator[Any, Any, Any], value: str) -> filter[str]:
    return filter(lambda logs_line: value in logs_line, logs)


def do_map(logs: Generator[Any, Any, Any], value: str) -> map[str]:
    return map(lambda logs_line: logs_line.split(' ')[int(value)], logs)


def do_unique(logs: Generator[Any, Any, Any]) -> set[str]:
    return set(logs)


def do_sort(logs: Generator[Any, Any, Any], value: str) -> List[str]:
    is_asc = value == 'asc'
    return sorted(logs, reverse=is_asc)


def do_limit(logs: Generator[Any, Any, Any], count: str) -> islice[str]:
    return itertools.islice(logs, 0, int(count))


def do_regex(logs: Generator[Any, Any, Any], regex: str) -> List[str]:
    reg = re.compile(r"{}".format(regex))
    return [line for line in logs if bool(reg.findall(line))]


@dataclass
class Query:
    file_name: str
    cmd1: str
    value1: str
    cmd2: str
    value2: str


def build_query(file: Generator[Any, Any, Any], query: Any) -> Any:
    result = perform_command(file, query.cmd1, query.value1)
    result = perform_command(result, query.cmd2, query.value2)
    return result if result else "Something went wrong"


dataSchema = marshmallow_dataclass.class_schema(Query)


def get_data(data: dict) -> Query:
    try:
        return dataSchema().load(data)
    except marshmallow.exceptions.ValidationError:
        raise ValueError


@app.route("/perform_query", methods=('POST', 'GET'))
def perform_query() -> Any:
    data = get_data(request.args.to_dict())
    file_name = data.file_name
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        return BadRequest(description=f"{file_name} does not exist")

    logs = read_file(file_path)

    result = build_query(logs, data)

    return app.response_class(result, content_type="text/plain")