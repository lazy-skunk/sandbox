import asyncio
import logging
import os
from typing import Literal, TypedDict, cast

import httpx
import uvicorn
from a2wsgi import WSGIMiddleware
from a2wsgi.wsgi import WSGIApp
from flask import Flask, jsonify, render_template, request
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.server.mcpserver import MCPServer
from starlette.routing import Mount
from starlette.types import ASGIApp

app = Flask(__name__)
mcp = MCPServer("shopping-list")
logger = logging.getLogger("uvicorn.error")


SYSTEM_PROMPT = """あなたは買い物リストの案内係です。日本語で答えてください。
買い物リストについて質問されたら、必ず list_shopping_items で確認してください。
検索条件に合う項目を答え、見つからなければその旨を伝えてください。
リストの項目や購入状況は検索結果に基づいて答えてください。
"""


Category = Literal["食品", "日用品"]


class ShoppingItem(TypedDict):
    name: str
    category: Category
    purchased: bool


# 学習用のサンプル。変更後はサーバーを再起動する。
SHOPPING_LIST: list[ShoppingItem] = [
    {"name": "米", "category": "食品", "purchased": True},
    {"name": "卵", "category": "食品", "purchased": False},
    {"name": "納豆", "category": "食品", "purchased": False},
    {"name": "豆腐", "category": "食品", "purchased": False},
    {"name": "ささみ", "category": "食品", "purchased": False},
    {"name": "整腸剤", "category": "日用品", "purchased": False},
    {"name": "歯みがき粉", "category": "日用品", "purchased": True},
    {"name": "ゴミ袋", "category": "日用品", "purchased": False},
]


@mcp.tool()
def list_shopping_items(
    category: Category | None = None,
    purchased: bool | None = None,
) -> list[ShoppingItem]:
    """買い物リストを検索する。

    category は「食品」または「日用品」。未指定なら全カテゴリが対象。
    purchased は購入済みなら true、未購入なら false。未指定なら両方が対象。
    """
    return [
        item.copy()
        for item in SHOPPING_LIST
        if (category is None or item["category"] == category)
        and (purchased is None or item["purchased"] == purchased)
    ]


async def generate_reply(prompt: str, trace: list) -> str:
    """ツール一覧を取得し、AI → MCP → AI の処理を実行する。"""
    mcp_url = os.getenv("MCP_URL", "http://127.0.0.1:8000/mcp")
    ollama_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
    model = os.getenv("OLLAMA_MODEL", "gemma4:e2b")
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]
    async with (
        streamable_http_client(mcp_url) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        logger.info("MCP: requesting tool list")
        discovered = (await session.list_tools()).tools
        logger.info("MCP: discovered %d tool(s)", len(discovered))
        tools = [
            {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description or "",
                    "parameters": tool.input_schema,
                },
            }
            for tool in discovered
        ]
        trace.append({"step": "MCP ツール一覧取得", "tools": tools})
        async with httpx.AsyncClient(timeout=180) as client:
            for round_number in range(1, 7):
                logger.info(
                    "Ollama: sending request %d (model: %s)",
                    round_number,
                    model,
                )
                response = await client.post(
                    f"{ollama_url.rstrip('/')}/api/chat",
                    json={
                        "model": model,
                        "messages": messages,
                        "tools": tools,
                        "stream": False,
                    },
                )
                response.raise_for_status()
                message = response.json()["message"]
                messages.append(message)
                calls = message.get("tool_calls") or []
                logger.info(
                    "Ollama: received response %d with %d tool call(s)",
                    round_number,
                    len(calls),
                )
                if not calls:
                    return message.get("content", "").strip()
                for call in calls:
                    function = call["function"]
                    name = function["name"]
                    arguments = function["arguments"]
                    if name not in {tool.name for tool in discovered}:
                        raise ValueError("AI が未登録のツールを指定しました。")
                    entry = {
                        "step": "AI → MCP ツール実行",
                        "tool": name,
                        "arguments": arguments,
                    }
                    trace.append(entry)
                    logger.info("MCP: calling %s", name)
                    result = await session.call_tool(name, arguments)
                    logger.info(
                        "MCP: %s finished (is_error: %s)",
                        name,
                        result.is_error,
                    )
                    output = result.model_dump(mode="json", by_alias=True)
                    entry["result"] = output
                    messages.append(
                        {
                            "role": "tool",
                            "tool_name": name,
                            "content": result.model_dump_json(by_alias=True),
                        }
                    )
    raise ValueError(
        "ツール呼び出しの上限に達しました。条件を絞ってください。"
    )


@app.route("/")
def index() -> str:
    return render_template(
        "index.html",
        items=SHOPPING_LIST,
    )


@app.post("/chat")
def chat():
    payload = request.get_json(silent=True)
    prompt = payload.get("message") if isinstance(payload, dict) else None
    if not isinstance(prompt, str) or not prompt.strip():
        return jsonify(error="質問を入力してください。", trace=[]), 400
    if len(prompt) > 4000:
        return jsonify(
            error="質問は4,000文字以内にしてください。", trace=[]
        ), 400
    trace = []
    try:
        logger.info("Chat: received question")
        answer = asyncio.run(generate_reply(prompt.strip(), trace))
        logger.info("Chat: returning answer")
        return jsonify(answer=answer, trace=trace)
    except Exception:
        logger.exception("AI / MCP communication failed")
        return jsonify(
            error="AI または MCP との通信に失敗しました。"
            "Ollama の起動・モデルの取得・接続先を確認してください。"
            "詳細はサーバーのログを確認してください。",
            trace=trace,
        ), 502


http_app = mcp.streamable_http_app(stateless_http=True)
flask_asgi = cast("ASGIApp", WSGIMiddleware(cast("WSGIApp", app)))
http_app.routes.append(Mount("/", app=flask_asgi))

if __name__ == "__main__":
    uvicorn.run(http_app, host="0.0.0.0", port=8000)
