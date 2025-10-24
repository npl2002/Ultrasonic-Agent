import yaml
import json
from pathlib import Path

# 输入路径
yaml_path = Path("mappings/nodes.yaml")
# 输出路径
json_path = Path("mappings/nodes.schema.json")

# 1. 读取 YAML
with open(yaml_path, "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)

# 2. 提取节点名（nodes 下所有 key）
node_keys = sorted(set(data.get("nodes", {}).keys()))

# 3. 生成 schema 结构
schema = {
    "$id": "mappings/nodes.schema.json",
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "node_id": {
            "type": "string",
            "enum": node_keys
        }
    }
}

# 4. 写入 JSON 文件
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(schema, f, ensure_ascii=False, indent=2)

print(f"✅ nodes.schema.json 已生成，共 {len(node_keys)} 个节点。")
