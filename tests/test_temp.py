#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --- add project root to sys.path ---
import sys, os
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]  # .../Agent训练
sys.path.insert(0, str(ROOT))
import json, os
from copy import deepcopy
from engine.loader import load_simulator

DEBUG = True  # 想关掉就改为 False

from engine.validators import validate_node_ready

def debug_gen_report(state, events):
    if not DEBUG:
        return
    print("\n[DEBUG] ----- GENERATE_REPORT validation -----")
    print("[DEBUG] events:", events)
    # 分别打印两个节点级门槛
    tri = validate_node_ready("TI-RADS", state)
    vis = validate_node_ready("VIS_REPORT", state)
    print("[DEBUG] TI-RADS -> passed:", tri["passed"], "errors:", tri["errors"], "warnings:", tri["warnings"])
    print("[DEBUG] VIS_REPORT -> passed:", vis["passed"], "errors:", vis["errors"], "warnings:", vis["warnings"])

    # 关键字段现况（是否存在/为空）
    def show_key(k):
        v = state.get(k, None)
        empty = v in (None, "", [], {})
        return f"{k}={'<MISSING>' if k not in state else ('<EMPTY>' if empty else repr(v)[:60])}"
    keys_to_check = [
        "state.tirads_score", "state.tirads_label",
        "state.conclusion_text", "state.recommendation",
        # 如果你怀疑是“旧字段名”，顺便看下这些：
        "state.ti_rads_score", "state.ti_rads_overall",
        "reports.conclusion_text", "reports.recommendation", "reports.follow_up_plan",
    ]
    print("[DEBUG] key snapshots:")
    for k in keys_to_check:
        print("   -", show_key(k))
    print("[DEBUG] --------------------------------------\n")


# 你给的病历 JSON（示例两例）
CASE_FILES = [
    ("P00001", "data/examples/patient_data_P00001.json"),
    ("P00002", "data/examples/patient_data_P00002.json"),
    ("P00003", "data/examples/patient_data_P00003.json"),
    ("P00004", "data/examples/patient_data_P00004.json"),
    # ("P00005", "data/examples/patient_data_P00005.json"),
]

OUT_DIR = "data/planner"
os.makedirs(OUT_DIR, exist_ok=True)

# 简单的观测挑选（避免把整个 state 写进去）
OBS_KEYS_CORE = [
    "state.thyroid_size", "state.thyroid_echo", "state.diffuse_lesion_evaluation_result",
    "state.thyroid_nodules_determined", "state.ti_rads_score",
    "state.tirads_score", "state.tirads_label",
    "reports.visual_report", "reports.structured_report",
]

def obs_keys_present(state):
    return [k for k in OBS_KEYS_CORE if k in state]

def record_step(sim, state, executed_nodes, action, steps):
    res = sim.step(state, executed_nodes, action)
    steps.append({
        "step_id": len(steps) + 1,
        "observation_keys": obs_keys_present(state),
        "action": action,
        "executed_nodes": deepcopy(res["executed_nodes"]),
        "events": res["events"],
        "done": (action["type"] == "GENERATE_REPORT" and res["ok"] is True)
    })
    if action["type"] == "GENERATE_REPORT":
        debug_gen_report(state, res["events"])  # ⬅️ 关键调试入口
    return res


def build_trajs_for_case(case_id, case_json):
    """
    从病例 JSON 构建该病例的若干黄金轨迹（episodes）。
    依赖外部的：load_simulator(), record_step(), obs_keys_present()
    返回：List[episode]
    """
    # ---------- 1) 初始化 & 字段归一化（映射到 validators 期望的键名） ----------
    base_state = {}
    # 把可能出现的信息源统一打平（按需增减）
    base_state.update(case_json.get("state", {}))
    base_state.update(case_json.get("reports", {}))
    base_state.update(case_json.get("labs", {}))
    base_state.update(case_json.get("ultrasound", {}))
    base_state.update(case_json.get("summary", {}))

    # ---- TI-RADS：score / label 至少二选一（统一到 state.* 命名）----
    if "state.tirads_label" not in base_state:
        if "state.ti_rads_overall" in base_state:
            base_state["state.tirads_label"] = base_state["state.ti_rads_overall"]
        elif "ti_rads_overall" in base_state:
            base_state["state.tirads_label"] = base_state["ti_rads_overall"]

    if "state.tirads_score" not in base_state:
        if "state.ti_rads_score" in base_state:
            base_state["state.tirads_score"] = base_state["state.ti_rads_score"]
        elif "ti_rads_score" in base_state:
            base_state["state.tirads_score"] = base_state["ti_rads_score"]
        # 兼容可能的历史细粒度键
        elif "state.thyroid_nodules_detail.nodules.ti_rads_score" in base_state:
            base_state["state.tirads_score"] = base_state["state.thyroid_nodules_detail.nodules.ti_rads_score"]
    
    # 标注无结节
    if "state.thyroid_nodules_quantity" in base_state:
        try:
            if int(base_state["state.thyroid_nodules_quantity"]) == 0:
                base_state.setdefault("state.no_nodule", True)
        except Exception:
            pass

    # 无结节时给更合理的默认报告（如果你没写）
    if base_state.get("state.no_nodule") is True:
        base_state.setdefault("state.conclusion_text", "甲状腺未见明确结节，TI-RADS 不适用。")
        base_state.setdefault("state.recommendation", "建议常规随访，出现临床症状或体征变化时复查。")

    # ---- 报告门槛：结论 & 建议（统一到 state.* 命名）----
    if "state.conclusion_text" not in base_state:
        if "reports.conclusion_text" in base_state:
            base_state["state.conclusion_text"] = base_state["reports.conclusion_text"]

    if "state.recommendation" not in base_state:
        if "reports.recommendation" in base_state:
            base_state["state.recommendation"] = base_state["reports.recommendation"]
        elif "reports.follow_up_plan" in base_state:
            base_state["state.recommendation"] = base_state["reports.follow_up_plan"]

    # ---------- 补默认报告字段（让验证器能通过） ----------
    base_state.setdefault("state.conclusion_text", "甲状腺结节 TI-RADS 4B 类，建议穿刺活检。")
    base_state.setdefault("state.recommendation", "建议 6 个月后复查或细针穿刺。")

    episodes = []

    # ---------- 2) 直达成功（两条，顺序稍作变化以增加多样性） ----------
    for idx in (1, 2):
        sim = load_simulator()
        state = deepcopy(base_state)
        executed = []
        steps = []

        order = [
            {"type": "EXECUTE", "node": "ORG_OVERVIEW"},
            {"type": "EXECUTE", "node": "DIFFUSE_EVAL"},
            {"type": "EXECUTE", "node": "NODULE_EVAL"},
            {"type": "EXECUTE", "node": "TI-RADS"},
            {"type": "EXECUTE", "node": "VIS_REPORT"},
            {"type": "GENERATE_REPORT"},
        ]
        if idx == 2:  # 交换 DIFFUSE 与 NODULE 的顺序
            order[1], order[2] = order[2], order[1]

        ok = True
        for act in order:
            res = record_step(sim, state, executed, act, steps)
            if act["type"] == "GENERATE_REPORT" and not res["ok"]:
                ok = False
        if ok:
            episodes.append({
                "case_id": case_id,
                "traj_id": f"{case_id}_direct_success_{idx}",
                "steps": steps
            })

    # ---------- 3) 澄清后成功（clarify → success） ----------
    sim = load_simulator()
    state = deepcopy(base_state)
    executed = []
    steps = []
    seq = [
        {"type": "EXECUTE", "node": "ORG_OVERVIEW"},
        {"type": "CLARIFY", "slot": "thyroid_nodules_detail"},
        {"type": "EXECUTE", "node": "NODULE_EVAL"},
        {"type": "EXECUTE", "node": "TI-RADS"},
        {"type": "EXECUTE", "node": "VIS_REPORT"},
        {"type": "GENERATE_REPORT"},
    ]
    ok = True
    for act in seq:
        res = record_step(sim, state, executed, act, steps)
        if act["type"] == "GENERATE_REPORT" and not res["ok"]:
            ok = False
    if ok:
        episodes.append({
            "case_id": case_id,
            "traj_id": f"{case_id}_clarify_then_success_1",
            "steps": steps
        })

    # ---------- 4) 误操作 → 回退 → 纠正（aggregate_only） ----------
    sim = load_simulator()
    state = deepcopy(base_state)
    executed = []
    steps = []
    seq = [
        {"type": "EXECUTE", "node": "ORG_OVERVIEW"},
        {"type": "EXECUTE", "node": "NODULE_EVAL"},
        {"type": "EXECUTE", "node": "VIS_REPORT"},  # 误操作：过早进入报告层
        {"type": "ROLLBACK", "node": "NODULE_EVAL", "policy": "aggregate_only"},
        {"type": "EXECUTE", "node": "TI-RADS"},
        {"type": "EXECUTE", "node": "VIS_REPORT"},
        {"type": "GENERATE_REPORT"},
    ]
    ok = True
    for act in seq:
        res = record_step(sim, state, executed, act, steps)
        if act["type"] == "GENERATE_REPORT" and not res["ok"]:
            ok = False
    if ok:
        episodes.append({
            "case_id": case_id,
            "traj_id": f"{case_id}_mistake_then_fix_aggr_1",
            "steps": steps
        })

    # ---------- 5) 误操作 → 回退 → 纠正（full_downstream） ----------
    sim = load_simulator()
    state = deepcopy(base_state)
    executed = []
    steps = []
    seq = [
        {"type": "EXECUTE", "node": "ORG_OVERVIEW"},
        {"type": "EXECUTE", "node": "DIFFUSE_EVAL"},
        {"type": "EXECUTE", "node": "VIS_REPORT"},  # 误操作
        {"type": "ROLLBACK", "node": "ORG_OVERVIEW", "policy": "full_downstream"},
        {"type": "EXECUTE", "node": "ORG_OVERVIEW"},
        {"type": "EXECUTE", "node": "DIFFUSE_EVAL"},
        {"type": "EXECUTE", "node": "NODULE_EVAL"},
        {"type": "EXECUTE", "node": "TI-RADS"},
        {"type": "EXECUTE", "node": "VIS_REPORT"},
        {"type": "GENERATE_REPORT"},
    ]
    ok = True
    for act in seq:
        res = record_step(sim, state, executed, act, steps)
        if act["type"] == "GENERATE_REPORT" and not res["ok"]:
            ok = False
    if ok:
        episodes.append({
            "case_id": case_id,
            "traj_id": f"{case_id}_mistake_then_fix_full_1",
            "steps": steps
        })

    return episodes


class Dog:

    def __init__(self, name: str, age: int, breed: str = "未知品种", action: str = "摇尾巴"):
        self.name = name
        self.age = age
        self.breed = breed
        self.action = action

    def bark(self):
        print(f"{self.name}：汪汪！")

    def eat(self, food: str):
        print(f"{self.name} 正在吃 {food}。")

    def get_older(self):
        self.age += 1
        print(f"{self.name} 过了一年，现在 {self.age} 岁了。")

    def action(self, action:str ):
        print(f"{self.name} 正在 {action}。")

    def __str__(self):
        """打印狗的信息"""
        return f"名字：{self.name}, 年龄：{self.age}, 品种：{self.breed}"

# 创建一只狗
dog1 = Dog(name="旺财", age=3, breed="金毛")

# 让狗叫
dog1.bark()

# 喂它吃东西
dog1.eat("骨头")

# 狗长大一岁
dog1.get_older()

# 打印狗的信息
print(dog1)


def main():
    sim = load_simulator()  # 只为触发一次 import/路径检查
    del sim

    all_eps = []
    for cid, fname in CASE_FILES:
        with open(fname, "r", encoding="utf-8") as f:
            case_json = json.load(f)
        eps = build_trajs_for_case(cid, case_json)
        all_eps.extend(eps)

    # 简单切分 train/val（偶数进 val）
    train_path = os.path.join(OUT_DIR, "train.jsonl")
    val_path   = os.path.join(OUT_DIR, "val.jsonl")
    with open(train_path, "w", encoding="utf-8") as ft, open(val_path, "w", encoding="utf-8") as fv:
        for i, ep in enumerate(all_eps):
            line = json.dumps(ep, ensure_ascii=False)
            (fv if i % 2 == 0 else ft).write(line + "\n")

    print(f"Wrote {len(all_eps)} episodes → {train_path}, {val_path}")

if __name__ == "__main__":
    main()
