from typing import Dict, Any, List, Literal, TypedDict, Optional, Set

ActionType = Literal["EXECUTE", "ROLLBACK", "CLARIFY", "GENERATE_REPORT"]

class Action(TypedDict, total=False):
    type: ActionType
    node: Optional[str]
    policy: Optional[str]
    slot: Optional[str]
    payload: Optional[Dict[str, Any]]  # 允许 EXECUTE 注入占位数据

class StepResult(TypedDict):
    ok: bool
    updated_state: Dict[str, Any]
    executed_nodes: List[str]
    events: List[str]  # 记录发生的事（便于调试/训练日志）
