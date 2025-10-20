import yaml
import json
from jsonschema import Draft202012Validator
from .simulator import Simulator
from pathlib import Path


def load_simulator() -> Simulator:
    # === 1. 加载 4 份规则文件 ===
    with open("mappings/nodes.yaml", "r", encoding="utf-8") as f:
        nodes_spec = yaml.safe_load(f)
    with open("rules/node_graph.yaml", "r", encoding="utf-8") as f:
        node_graph = yaml.safe_load(f)["edges"]
    with open("rules/rollback_rules.yaml", "r", encoding="utf-8") as f:
        rollback_rules = yaml.safe_load(f)
    with open("rules/rollback_policies.yaml", "r", encoding="utf-8") as f:
        rollback_policies = yaml.safe_load(f)

    # === 2. 加载动作 Schema ===
    with open("schemas/action.schema.json", "r", encoding="utf-8") as f:
        action_schema = json.load(f)
    validator = Draft202012Validator(action_schema)

    """
    # === 2. 加载动作 Schema ===
    schema_path = Path("schemas/action.schema.json").resolve()
    print(f"[loader] using action schema: {schema_path}")
    with open(schema_path, "r", encoding="utf-8") as f:
        action_schema_text = f.read()
    print("[loader] action schema head:", action_schema_text[:120].replace("\n", " "))
    action_schema = json.loads(action_schema_text)
    validator = Draft202012Validator(action_schema)
    """

    # === 3. 初始化模拟器 ===
    sim = Simulator(nodes_spec, node_graph, rollback_rules, rollback_policies)

    # === 4. 包装 step()：执行前做 schema 校验 ===
    original_step = sim.step

    def step_with_validation(state, executed_nodes, action):
        # 校验 action
        """
        errors = sorted(validator.iter_errors(action), key=lambda e: e.path)
        if errors:
            messages = [
                f"{'/'.join(map(str, e.path))}: {e.message}" for e in errors
            ]
            return {
                "ok": False,
                "updated_state": state,
                "executed_nodes": executed_nodes,
                "events": [f"SchemaError: {', '.join(messages)}"],
            }
        """
        errors = sorted(validator.iter_errors(action), key=lambda e: e.path)
        if errors:
            lines = []
            for e in errors:
                path = "/" + "/".join(map(str, e.path))
                lines.append(f"{path or '/'}: {e.message}")
            return {
                "ok": False,
                "updated_state": state,
                "executed_nodes": executed_nodes,
                "events": [f"SchemaError: {' | '.join(lines)}"],
            }


        # 校验通过，执行原始逻辑
        return original_step(state, executed_nodes, action)

    # 替换原来的 step 方法
    sim.step = step_with_validation

    return sim
