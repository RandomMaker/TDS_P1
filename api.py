import urllib.parse
import json
import logging
import traceback
from typing import Callable, Dict
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pathlib import Path
import os
from fastapi.responses import PlainTextResponse
import requests
from function_tasks import (
    convert_function_to_openai_schema,
    count_occurrences,
    extract_specific_content_and_create_index,
    extract_specific_text_using_llm,
    extract_text_from_image,
    format_file_with_prettier,
    get_similar_text_using_embeddings,
    install_and_run_script,
    process_and_write_logfiles,
    query_database,
    sort_json_by_keys,
)


load_dotenv()
API_KEY = os.getenv("AIPROXY_TOKEN")
URL_CHAT = "https://aiproxy.sanand.workers.dev/openai/v1/chat/completions"
URL_EMBEDDING = "https://aiproxy.sanand.workers.dev/openai/v1/embeddings"

app = FastAPI()

logging.basicConfig(level=logging.INFO)

DATA_DIR = Path("../")
DATA_DIR.mkdir(exist_ok=True)  # Ensure data directory exists

function_mappings: Dict[str, Callable] = {
    "install_and_run_script": install_and_run_script,
    "format_file_with_prettier": format_file_with_prettier,
    "count_occurrences": count_occurrences,
    "sort_json_by_keys": sort_json_by_keys,
    "process_and_write_logfiles": process_and_write_logfiles,
    "extract_specific_content_and_create_index": extract_specific_content_and_create_index,
    "extract_specific_text_using_llm": extract_specific_text_using_llm,
    "extract_text_from_image": extract_text_from_image,
    "get_similar_text_using_embeddings": get_similar_text_using_embeddings,
    "query_database": query_database,
}


def parse_task_description(task_description: str, tools: list):
    response = requests.post(
        URL_CHAT,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": "gpt-4o-mini",
            "messages": [
                {
                    "role": "system",
                    "content": "You are an intelligent agent fluent in multiple human languages that understands and parses tasks. If a task is given to you in any language other than english, you first translate the task to english by yourself and then you quickly identify the best tool funcions to use to give the desired results using minimal functions",
                },
                {"role": "user", "content": task_description},
            ],
            "tools": tools,
            "tool_choice": "required",
        },
    )
    # logging.info(
    # "********************************************** PRINTING RESPONSE START************************************"
    # )
    # print(response.json())
    # logging.info(
    # "********************************************** PRINTING RESPONSE END************************************"
    # )
    return response.json()["choices"][0]["message"]


def execute_function_call(function_call):
    logging.info(f"INSIDE execute_function_call with function_call: {function_call}")
    try:
        function_name = function_call["name"]
        function_args = json.loads(function_call["arguments"])
        function_to_call = function_mappings.get(function_name)
        # print("Calling function: ", function_name)
        # print("Arguments ", function_args)
        if function_to_call:
            function_to_call(**function_args)
        else:
            raise ValueError(f"Function {function_name} was not found")
    except Exception as e:
        error_details = traceback.format_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error executing function in execute_function_call: {str(e)}",
            headers={"X-Traceback": error_details},
        )


@app.post("/run")
def run_task(task: str = Query(..., description="Task description")):
    task = urllib.parse.quote(task)
    tools = [
        convert_function_to_openai_schema(func) for func in function_mappings.values()
    ]

    # logging.info(
    # f"############################Inside run_task with task: {task} ##########################"
    # )

    try:
        function_call_response_message = parse_task_description(task, tools)
        # print(function_call_response_message)
        if function_call_response_message["tool_calls"]:
            for tool in function_call_response_message["tool_calls"]:
                execute_function_call(tool["function"])
        return {"status": "success", "message": "Task executed successfully"}
    except Exception as e:
        error_details = traceback.format_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error executing function in run_task: {str(e)}",
            headers={"X-Traceback": error_details},
        )


@app.get("/read", response_class=PlainTextResponse)
def read_file(path: str = Query(..., description="File path to read")):
    file_path = DATA_DIR / Path(path)
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return file_path.read_text()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
