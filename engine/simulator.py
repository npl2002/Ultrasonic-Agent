from typing import Dict, Any, List, Set, Optional
from .types import Action, StepResult
from .validators import validate_node_ready

class Simulator:
    def __init__(
        self,
        nodes_spec: Dict[str, Any],            # mappings/nodes.yaml 解析后的字典
        node_graph: Dict[str, List[str]],      # rules/node_graph.yaml 的 edges 映射 {upstream: [downstreams]}
        rollback_rules: Dict[str, Any],        # rules/rollback_rules.yaml
        rollback_policies: Dict[str, Any],     # rules/rollback_policies.yaml
    ):
        self.nodes_spec = nodes_spec
        self.node_graph = node_graph
        self.rollback_rules = rollback_rules
        self.rollback_policies = rollback_policies

        # 预构建：节点 -> produces 集合；以及 reports_fixed、aggregate_nodes
        self._produces = {
            n: set((spec or {}).get("produces", []))
            for n, spec in (self.nodes_spec.get("nodes", {}) or {}).items()
        }
        self._all_nodes = set(self._produces.keys())
        self._reports_fixed = set(self.rollback_rules.get("reports_fixed", []) or [])

        # 支持两种结构：policies 顶层或 policy.definitions（兼容你之前的两版）
        self._aggregate_nodes = set(
            self.rollback_policies.get("aggregate_nodes", [])
            or (self.rollback_policies.get("policy", {}) or {}).get("aggregate_nodes", [])
        )

        # 归一化：rollback_rules.rules 映射
        self._rule_clears = (self.rollback_rules.get("rules", {}) or {})

    # ---------------- core helpers ----------------

    def _write_set(self, node: str) -> List[str]:
        # 对齐 nodes.yaml：从 produces 取写字段
        return list(self._produces.get(node, set()))

    def _downstream_nodes(self, node: str) -> List[str]:
        """拓展 node 的所有下游（不含自身）"""
        seen, q = set(), [node]
        while q:
            cur = q.pop()
            for nxt in self.node_graph.get(cur, []):
                if nxt not in seen:
                    seen.add(nxt)
                    q.append(nxt)
        seen.discard(node)
        return list(seen)

    def _clears_full_downstream(self, node: str) -> List[str]:
        """clear = node + downstream nodes 的 clears 联集；如果 rules 缺少某节点条目，则退化为它自身 produces + reports_fixed"""
        targets = [node] + self._downstream_nodes(node)
        fields: Set[str] = set()
        for n in targets:
            entry = self._rule_clears.get(n, {})
            clears = entry.get("clears", [])
            if clears:
                fields.update(clears)
            else:
                # 兜底：至少把该节点 produces 与 reports_fixed 清掉
                fields.update(self._produces.get(n, set()))
        fields.update(self._reports_fixed)
        return list(sorted(fields))

    def _clears_aggregate_only(self, node: str) -> List[str]:
        """clear = node 自身 produces + 可达的聚合节点 produces + reports_fixed"""
        fields: Set[str] = set(self._produces.get(node, set()))
        for d in self._downstream_nodes(node):
            if d in self._aggregate_nodes:
                fields.update(self._produces.get(d, set()))
        fields.update(self._reports_fixed)
        return list(sorted(fields))

    def _apply_clear(self, state: Dict[str, Any], fields: List[str]) -> None:
        for f in fields:
            state.pop(f, None)

    # ---------------- public step() ----------------

    def step(
        self,
        state: Dict[str, Any],
        executed_nodes: List[str],
        action: Action,
    ) -> StepResult:
        events: List[str] = []
        at = action.get("type")

        if at == "EXECUTE":
            node = action["node"]
            ws = self._write_set(node)
            payload = action.get("payload", {}) or {}
            for f in ws:
                # 写占位：优先 payload，其次保留旧值，否则置 None
                state[f] = payload.get(f, state.get(f, None))
            if node not in executed_nodes:
                executed_nodes.append(node)
            events.append(f"EXECUTE {node}: wrote {len(ws)} fields")

        elif at == "ROLLBACK":
            node = action["node"]
            policy = action.get("policy", "full_downstream")

            if policy == "aggregate_only":
                fields = self._clears_aggregate_only(node)
                self._apply_clear(state, fields)
                # 移除自身与可达聚合节点
                rm_nodes = set([node] + [d for d in self._downstream_nodes(node) if d in self._aggregate_nodes])
                executed_nodes[:] = [n for n in executed_nodes if n not in rm_nodes]
                events.append(f"ROLLBACK {node} aggregate_only: cleared {len(fields)} fields; removed nodes={sorted(rm_nodes)}")

            elif policy == "full_downstream":
                fields = self._clears_full_downstream(node)
                # 受影响节点 = node + 所有下游
                affected_nodes = set([node] + self._downstream_nodes(node))
                self._apply_clear(state, fields)
                executed_nodes[:] = [n for n in executed_nodes if n not in affected_nodes]
                events.append(f"ROLLBACK {node} full_downstream: cleared {len(fields)} fields; removed nodes={sorted(affected_nodes)}")

            elif policy == "custom":
                # 支持三种自定义：指定字段、包含节点、排除节点
                include_nodes = set(action.get("include_nodes", []) or [])
                exclude_nodes = set(action.get("exclude_nodes", []) or [])
                fields = set(action.get("fields", []) or [])

                # 若给了 include_nodes，就把这些节点的 produces 并入
                for n in include_nodes:
                    fields.update(self._produces.get(n, set()))
                # 默认也清报告固定集，除非 action 里明确关闭
                if action.get("include_reports_fixed", True):
                    fields.update(self._reports_fixed)

                self._apply_clear(state, list(fields))
                # 从 executed_nodes 移除 include_nodes（不移除 exclude_nodes）
                if include_nodes:
                    executed_nodes[:] = [n for n in executed_nodes if n not in include_nodes]
                events.append(f"ROLLBACK {node} custom: cleared {len(fields)} fields; include_nodes={sorted(include_nodes)} exclude_nodes={sorted(exclude_nodes)}")

            else:
                return {"ok": False, "updated_state": state, "executed_nodes": executed_nodes,
                        "events": events + [f"Unknown policy {policy}"]}

        elif at == "CLARIFY":
            slot = action["slot"]
            pending = set(state.get("_pending_slots", []))
            pending.add(slot)
            state["_pending_slots"] = list(sorted(pending))
            events.append(f"CLARIFY {slot}")

        elif at == "GENERATE_REPORT":
            # 先校验 TI-RADS（若你的流水线要求 TI-RADS 先于可视化/结构化报告）
            tri = validate_node_ready("TI-RADS", state)
            vis = validate_node_ready("VIS_REPORT", state)

            passed = tri["passed"] and vis["passed"]
            errs = [f"TI-RADS: {e}" for e in tri["errors"]] + [f"VIS_REPORT: {e}" for e in vis["errors"]]
            warns = [f"TI-RADS: {w}" for w in tri["warnings"]] + [f"VIS_REPORT: {w}" for w in vis["warnings"]]

            events.append(f"GENERATE_REPORT validated: passed={passed}")
            if errs: events += [f"ERROR: {m}" for m in errs]
            if warns: events += [f"WARNING: {m}" for m in warns]

            return {"ok": passed, "updated_state": state, "executed_nodes": executed_nodes, "events": events}

        else:
            return {"ok": False, "updated_state": state, "executed_nodes": executed_nodes,
                    "events": events + ["Unknown action type"]}

        return {"ok": True, "updated_state": state, "executed_nodes": executed_nodes, "events": events}
