# scripts/ggenerate_node_payload_schemas.py
from pathlib import Path
import json
import yaml

ROOT = Path(__file__).resolve().parents[1]
YAML_PATH = ROOT / "mappings" / "nodes.yaml"
OUT_DIR = ROOT / "schemas" / "node_payloads"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def to_schema_for_node(node_name: str, consumes):
    """
    基于 consumes 列表生成一个 payload schema：
    - properties：consumes 中的字段都允许出现（默认类型 string）
    - required：不强制（澄清一般允许只提供部分字段）
    - minProperties: 1（至少澄清一个键）
    - additionalProperties: false（禁止无关字段）
    """
    # 兜底：如果 consumes 为空，允许空对象，但不强制任何字段
    props = {k: {"type": "string"} for k in (consumes or [])}

    schema = {
        "$id": f"schemas/node_payloads/{node_name}.schema.json",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {},
        "title": f"{node_name} payload",
        "type": "object",
        "$anchor": "payload",
        "properties": props,
        "additionalProperties": False,
    }

    # 如果有 consumes，就要求至少提供一个字段
    if consumes:
        schema["minProperties"] = 1
    return schema

def main():
    if not YAML_PATH.exists():
        raise FileNotFoundError(f"找不到 {YAML_PATH}")

    data = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8")) or {}
    nodes = (data.get("nodes") or {})
    if not isinstance(nodes, dict) or not nodes:
        raise ValueError("nodes.yaml 中未找到有效的 nodes 映射")

    count = 0
    for node_name, meta in nodes.items():
        # 约定：澄清值结构 = 该节点的输入字段集合（consumes）
        consumes = meta.get("consumes") or []
        if not isinstance(consumes, list):
            consumes = []

        schema = to_schema_for_node(node_name, consumes)
        out_path = OUT_DIR / f"{node_name}.schema.json"
        out_path.write_text(json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")
        count += 1

    print(f"✅ 已为 {count} 个节点生成澄清值 schema 到 {OUT_DIR}")

if __name__ == "__main__":
    main()
