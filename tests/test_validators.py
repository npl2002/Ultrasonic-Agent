# tests/test_validators.py
from engine.validators import validate_state_for_report
from engine.validators import validate_node_ready

def test_report_missing_minimals():
    state = {}
    r = validate_state_for_report(state)
    assert not r["passed"]
    assert any("Missing required" in e for e in r["errors"])
    assert any("Require at least one of" in e for e in r["errors"])

def test_report_minimal_ok_with_score_only():
    state = {
        "state.tirads_score": 4,
        "state.conclusion_text": "良性概率高",
        "state.recommendation": "6-12个月复查"
    }
    r = validate_state_for_report(state)
    assert r["passed"], r

def test_mutex_violation():
    state = {
        "state.tirads_score": 5,
        "state.conclusion_text": "恶性概率高",
        "state.recommendation": "外科评估",
        "state.conclusion_benign": True,
        "state.conclusion_malignant": True,
    }
    r = validate_state_for_report(state)
    assert not r["passed"]
    assert any("Mutually exclusive" in e for e in r["errors"])

def test_score_label_both_present_gives_warning():
    state = {
        "state.tirads_score": 5,
        "state.tirads_label": "TR4",   # 故意不一致，期望 warnings
        "state.conclusion_text": "建议进一步穿刺",
        "state.recommendation": "FNA+MDT"
    }
    r = validate_state_for_report(state)
    assert r["passed"]   # 仍可通过
    assert len(r["warnings"]) >= 1

def test_tirads_missing_both_score_and_label():
    state = {}
    r = validate_node_ready("TI-RADS", state)
    assert not r["passed"]
    assert any("Require at least one of" in e for e in r["errors"])

def test_tirads_label_only_ok():
    state = {"state.tirads_label": "TR5"}
    r = validate_node_ready("TI-RADS", state)
    assert r["passed"]