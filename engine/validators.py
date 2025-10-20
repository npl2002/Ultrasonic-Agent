# engine/validators.py
from typing import Dict, List, TypedDict, Optional
import re

class ValidateResult(TypedDict):
    passed: bool
    errors: List[str]
    warnings: List[str]

# === 字段别名（兼容历史键名） ===
KEY_ALIASES: Dict[str, List[str]] = {
    "state.tirads_score": [
        "state.ti_rads_score",
        "ti_rads_score",
        # 历史细粒度整体键（如有）
        "state.thyroid_nodules_detail.nodules.ti_rads_score_overall",
    ],
    "state.tirads_label": [
        "state.ti_rads_overall",
        "ti_rads_overall",
    ],
    "state.conclusion_text": [
        "reports.conclusion_text",
    ],
    "state.recommendation": [
        "reports.recommendation",
        "reports.follow_up_plan",
    ],
    "state.thyroid_nodules_quantity": [
        "thyroid_nodules_quantity",
    ],
}

def _get(state: Dict, key: str) -> Optional[object]:
    """带别名取值：先取主键，再按别名顺序取值。"""
    if key in state:
        return state[key]
    for alt in KEY_ALIASES.get(key, []):
        if alt in state:
            return state[alt]
    return None

def _present(state: Dict, key: str) -> bool:
    v = _get(state, key)
    return v not in (None, "", [], {})

# —— VIS_REPORT 的最小必填 —— #
REQUIRED_FOR_REPORT = [
    "state.conclusion_text",  # 结论
    "state.recommendation",   # 建议/随访
]

# 互斥组合（任一组合同时出现即错误）
MUTEX_GROUPS = [
    ["state.conclusion_benign", "state.conclusion_malignant"],
    ["state.func_hyper", "state.func_hypo"],
]

# === 无结节判断 ===
def _is_no_nodule(state: Dict) -> bool:
    q = _get(state, "state.thyroid_nodules_quantity")
    try:
        return q is not None and int(q) == 0
    except Exception:
        return False

# === 是否存在“任一结节”的 TI-RADS 分级 ===
def _has_per_nodule_tirads(state: Dict) -> bool:
    # 候选路径（按优先级从高到低）
    candidates = [
        ("state.thyroid_nodules_detail", "nodules"),
        ("thyroid_nodules_detail", "nodules"),
        ("state", "nodules"),
        ("ultrasound", "nodules"),
        (None, "nodules"),  # 顶层 nodules
    ]
    for head, tail in candidates:
        container = state.get(head) if head else state
        if not isinstance(container, dict):
            continue
        nods = container.get(tail) or []
        if not isinstance(nods, list):
            continue
        for n in nods:
            if not isinstance(n, dict):
                continue
            s = n.get("ti_rads_score") or n.get("tirads_score") or n.get("ti_rads") or n.get("tirads")
            if s not in (None, "", [], {}):
                return True
    return False

# === TI-RADS 等级比较的小工具（用于一致性提示） ===
_TIRADS_ORDER = {
    "1":1, "2":2, "3":3, "4A":4, "4B":5, "4C":6, "5":7,
    "TR1":1, "TR2":2, "TR3":3, "TR4A":4, "TR4B":5, "TR4C":6, "TR5":7
}
def _rank(val: Optional[str]) -> Optional[int]:
    if val is None:
        return None
    s = str(val).upper().strip()
    m = re.match(r"^(?:TI-?RADS\s*)?(TR)?\s*([1-5])\s*([ABC])?$", s)
    if m:
        base = m.group(2)
        suffix = m.group(3) or ""
        key = (f"{base}{suffix}" if not m.group(1) else f"TR{base}{suffix}")
        return _TIRADS_ORDER.get(key)
    return _TIRADS_ORDER.get(s)

# === VIS_REPORT 节点的最小门槛 ===
def validate_state_for_report(state: Dict) -> ValidateResult:
    errors: List[str] = []
    warnings: List[str] = []

    # 1) 必填：结论 + 建议
    for k in REQUIRED_FOR_REPORT:
        if not _present(state, k):
            errors.append(f"Missing required: {k}")

    # 2) 有/无结节分支
    if _is_no_nodule(state):
        # 无结节：不强制 TI-RADS；建议补充腺体/弥漫性信息
        if not (_present(state, "state.thyroid_echo") or _present(state, "state.diffuse_lesion_evaluation_result")):
            warnings.append("No nodule: consider adding gland echo or diffuse evaluation.")
    else:
        # 有结节：允许两种证据其一（整体或任一结节级）
        overall_ok = _present(state, "state.tirads_score") or _present(state, "state.tirads_label")
        per_nodule_ok = _has_per_nodule_tirads(state)
        if not (overall_ok or per_nodule_ok):
            errors.append("Need TI-RADS evidence: either overall (state.tirads_*) or per-nodule ti_rads_score.")
        elif per_nodule_ok and not overall_ok:
            warnings.append("Per-nodule TI-RADS only (no overall grading).")

    # 3) 互斥
    for group in MUTEX_GROUPS:
        pres = [k for k in group if _present(state, k)]
        if len(pres) > 1:
            errors.append(f"Mutually exclusive fields present: {pres}")

    # 4) 轻度一致性：仅当“整体”同时存在时比较
    if _present(state, "state.tirads_score") and _present(state, "state.tirads_label"):
        score = _get(state, "state.tirads_score")
        label = _get(state, "state.tirads_label")
        r_score = _rank(str(score)) if score is not None else None
        r_label = _rank(str(label)) if label is not None else None
        if r_score is None and isinstance(score, str):
            warnings.append("Non-standard tirads_score; check format.")
        if r_label is None and isinstance(label, str):
            warnings.append("Non-standard tirads_label; check format.")
        if (r_score is not None and r_label is not None) and r_score != r_label:
            warnings.append("TI-RADS label not aligned with score (ranks differ).")

    return {"passed": len(errors) == 0, "errors": errors, "warnings": warnings}

# === 节点级验证入口（与模拟器对接） ===
def validate_node_ready(node: str, state: Dict) -> ValidateResult:
    if node == "VIS_REPORT":
        return validate_state_for_report(state)

    if node == "TI-RADS":
        # 无结节：允许跳过 TI-RADS，给 warning
        if _is_no_nodule(state):
            return {"passed": True, "errors": [], "warnings": ["TI-RADS skipped: no discrete thyroid nodule."]}

        errors: List[str] = []
        warnings: List[str] = []

        overall_ok = _present(state, "state.tirads_score") or _present(state, "state.tirads_label")
        per_nodule_ok = _has_per_nodule_tirads(state)

        if not (overall_ok or per_nodule_ok):
            errors.append("Need TI-RADS: overall (state.tirads_*) or per-nodule ti_rads_score.")
        elif per_nodule_ok and not overall_ok:
            warnings.append("Per-nodule TI-RADS only (no overall grading).")

        # 仅当整体存在时做轻度一致性检查
        if overall_ok and _present(state, "state.tirads_score") and _present(state, "state.tirads_label"):
            score = _get(state, "state.tirads_score")
            label = _get(state, "state.tirads_label")
            r_score = _rank(str(score)) if score is not None else None
            r_label = _rank(str(label)) if label is not None else None
            if r_score is None and isinstance(score, str):
                warnings.append("Non-standard tirads_score; check format.")
            if r_label is None and isinstance(label, str):
                warnings.append("Non-standard tirads_label; check format.")
            if (r_score is not None and r_label is not None) and r_score != r_label:
                warnings.append("TI-RADS label not aligned with score (ranks differ).")

        return {"passed": len(errors) == 0, "errors": errors, "warnings": warnings}

    # 其它节点后续逐步补齐：默认放行
    return {"passed": True, "errors": [], "warnings": []}
