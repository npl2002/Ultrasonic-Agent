from engine.loader import load_simulator

def test_execute_and_rollback_full():
    sim = load_simulator()
    state, executed = {}, []

    # 执行一个节点（接收返回值）
    res = sim.step(state, executed, {"type": "EXECUTE", "node": "NODULE_EVAL"})

    # 打印调试信息
    print("\n[DEBUG] Action events:", res.get("events"))
    print("[DEBUG] ok =", res.get("ok"))
    print("[DEBUG] executed_nodes after step:", executed)

    # 断言执行是否成功
    assert res["ok"], f"Action failed: {res}"
    assert "NODULE_EVAL" in executed

def test_aggregate_only_scope():
    sim = load_simulator()
    state, executed = {}, ["NODULE_EVAL", "TI-RADS", "VIS_REPORT"]
    res = sim.step(state, executed, {"type": "ROLLBACK", "node": "NODULE_EVAL", "policy": "aggregate_only"})
    assert "TI-RADS" not in res["executed_nodes"]

def test_custom_policy():
    sim = load_simulator()
    state, executed = {}, ["NODULE_EVAL"]
    res = sim.step(state, executed, {
        "type": "ROLLBACK",
        "node": "NODULE_EVAL",
        "policy": "custom",
        "include_nodes": ["TI-RADS"],
        "fields": ["state.invasion"]
    })
    assert "state.invasion" not in res["updated_state"]

def test_unknown_policy():
    sim = load_simulator()
    state, executed = {}, []
    res = sim.step(state, executed, {"type": "ROLLBACK", "node": "NODULE_EVAL", "policy": "bad_mode"})
    assert not res["ok"]

def test_invalid_action_missing_policy():
    sim = load_simulator()
    res = sim.step({}, [], {"type": "ROLLBACK", "node": "NODULE_EVAL"})  # 缺 policy
    assert not res["ok"]
    assert "SchemaError" in res["events"][0]

def test_invalid_custom_action_fields():
    sim = load_simulator()
    res = sim.step({}, [], {
        "type": "ROLLBACK", "node": "NODULE_EVAL", "policy": "custom"
    })  # custom 但没 include_nodes/fields
    assert not res["ok"]
    assert "SchemaError" in res["events"][0]
