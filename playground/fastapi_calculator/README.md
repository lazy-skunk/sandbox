# FastAPI の計算 API サンプル

## HTTP リクエストの流れ

```mermaid
sequenceDiagram
    participant Client as curl／Locust
    participant Server as Uvicorn
    participant App as FastAPI
    Client->>Server: POST /calculate
    Server->>App: リクエストを渡す
    App->>App: 入力を検証して計算する
    App-->>Server: レスポンスを返す
    Server-->>Client: HTTP レスポンスを送る
```

## 動かし方

### FastAPI

```sh
uvicorn app:app --host 0.0.0.0 --port 8000
```

### Swagger UI

http://127.0.0.1:8000/docs

**Authorize** に `sample-token` を入力し、**Try it out** で試す。

### Locust UI

FastAPI を起動したまま、別のターミナルで実行する。

```sh
locust --host http://127.0.0.1:8000
```

http://127.0.0.1:8089

### curl

```sh
curl -i http://127.0.0.1:8000/calculate \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer sample-token' \
  --data '[
    {"num_a": 3, "num_b": 4, "method": "add"},
    {"num_a": 3, "num_b": 4, "method": "multiply"}
  ]'
```

## 計算 API

```mermaid
flowchart TD
    Request["POST /calculate<br/>num_a・num_b・method の配列（最大100組）"]
    Request --> Auth{"Bearer トークンが一致？"}
    Auth -->|いいえ| Unauthorized["401 Unauthorized"]
    Auth -->|はい| Validate{"入力が有効？"}
    Validate -->|いいえ| Error["422 入力エラー<br/>detail に理由"]
    Validate -->|はい| Method{"組ごとの method"}
    Method -->|add| Add["num_a + num_b"]
    Method -->|multiply| Multiply["num_a × num_b"]
    Add --> Response["200 Success<br/>results.scores に入力順の結果"]
    Multiply --> Response
```
