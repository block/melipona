# Synthetic stdio peer for adapter boundary tests. No network or credentials.
import json
import hashlib
import os
import subprocess
import sys
import time

mode = sys.argv[1]
marker = sys.argv[2] if len(sys.argv) > 2 else None

def send(value):
    print(json.dumps(value), flush=True)

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    ident = message.get("id")
    if method == "initialize":
        if mode == "environment":
            with open(marker, "w") as output:
                json.dump({"providerEnvPresent": any(k.startswith("REALTIME_") for k in os.environ),
                           "configured": os.environ.get("MCP_FIXTURE_VALUE")}, output)
        if mode == "hang_init":
            time.sleep(60)
        send({"jsonrpc": "2.0", "id": ident, "result": {
            "protocolVersion": message["params"]["protocolVersion"],
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "fixture", "version": "1"}}})
    elif method == "tools/list":
        schema = {"type": "object"}
        if mode == "bad_schema":
            schema = {"$ref": "https://example.invalid/schema"}
        names = ["echo"]
        if mode == "names":
            names = ["echo", "dot.name", "embedded__separator", "long" * 40]
        if mode == "collision":
            original = "x" * 70
            alias = ("dev__" + original)[:47] + "_" + hashlib.sha256(("dev\0" + original).encode()).hexdigest()[:16]
            names = [original, alias[len("dev__"):]]
        if mode == "schema_prefix":
            names = ["a", "ab"]
        result = {"tools": [{"name": name, "inputSchema": ({"$ref": "https://example.invalid/schema"} if mode == "schema_prefix" and name == "ab" else schema),
                  "annotations": {"destructiveHint": True}} for name in names]}
        if mode == "cycle":
            result["nextCursor"] = "same"
        send({"jsonrpc": "2.0", "id": ident, "result": result})
    elif method == "tools/call":
        if marker:
            with open(marker, "a") as output:
                output.write("called\n")
        if mode == "oversize":
            send({"jsonrpc":"2.0","id":ident,"result":{"content":[{"type":"text","text":"x" * 16384}]}})
            continue
        if mode == "malformed":
            print("not json", flush=True)
            continue
        if mode == "pending":
            continue
        if mode in ("descendant", "exit_on_eof", "exit_on_call"):
            child = subprocess.Popen(["sleep", "60"])
            with open(marker, "a") as output:
                output.write(str(child.pid) + "\n")
                if mode != "descendant":
                    output.write("leader=" + str(os.getpid()) + "\n")
            if mode == "exit_on_call":
                break
            continue
        if mode == "protocol_error":
            send({"jsonrpc": "2.0", "id": ident, "error": {"code": -32603, "message": "fixture error"}})
            continue
        if mode == "delayed":
            time.sleep(0.15)
        result = {"content": [{"type": "text", "text": mode}],
                  "structuredContent": {"name": message["params"]["name"],
                  "arguments": message["params"].get("arguments"),
                  "providerEnvPresent": any(k.startswith("REALTIME_") for k in os.environ)},
                  "isError": mode == "application_error"}
        if mode == "large":
            result["content"][0]["text"] = "界\\\"" * 5000
        if mode == "image":
            result["content"].append({"type": "image", "mimeType": "image/png", "data": "c2VjcmV0"})
        send({"jsonrpc": "2.0", "id": ident, "result": result})
    elif method == "notifications/cancelled":
        if marker:
            with open(marker, "a") as output:
                output.write("cancelled\n")
if mode == "descendant":
    time.sleep(60)
