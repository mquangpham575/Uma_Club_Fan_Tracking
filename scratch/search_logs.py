import json

log_path = r"C:\Users\QUANG\.gemini\antigravity-ide\brain\3e1a753c-97bd-4058-99d9-40790e8a2e6c\.system_generated\logs\transcript.jsonl"

with open(log_path, 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        if any(w in line.lower() for w in ["white", "blue", "seprate", "separate", "color"]):
            # Print index and a snippet of the content
            try:
                data = json.loads(line)
                content = data.get("content", "")
                if "user" in data.get("source", "").lower() or "planner" in data.get("type", "").lower():
                    print(f"Line {i} ({data.get('source')}): {content[:200]}")
            except Exception:
                pass
