#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Validate nodes registry & rules coherence.

Checks implemented:
1) Structural checks
   - node_graph must be acyclic; all nodes in edges exist in nodes.yaml
   - rollback_rules: every node entry key exists; 'clears'/'reset_set' subset of "universe of fields"
   - rollback_policies: policy names only from allowed set; aggregate_nodes exist

2) Semantic checks
   - Uniqueness: each produced field must be owned by exactly one node
   - No write conflicts along graph: for any A->B reachable, produces[A] ∩ produces[B] == ∅
   - full_downstream: clears[N] ⊇ produces[N] ∪ produces[Reach(N)] ∪ reports_fixed
   - aggregate_only: for each N, simulated set = produces[N] ∪ (produces of aggregate_nodes reachable from N) ∪ reports_fixed
                     must not include non-aggregate nodes' produced fields (except produces[N])
   - Coverage report: list fields that are not covered by ANY rollback policy union
                      (union over simulated 'full_downstream' for all N; union over simulated 'aggregate_only' for all N;
                       'custom' is caller-defined; 'diagnostic_tiers' is workflow-specific so only noted)

Outputs:
- Human-readable report to stdout
- CSV coverage table to --out/coverage_report.csv
"""

import argparse
import os
import sys
import yaml
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple

ALLOWED_POLICIES = {"aggregate_only", "full_downstream", "custom", "diagnostic_tiers"}

def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def topo_check_acyclic(nodes: Set[str], edges_map: Dict[str, List[str]]) -> Tuple[bool, List[str]]:
    in_deg = {n: 0 for n in nodes}
    for a, outs in edges_map.items():
        for b in outs:
            in_deg[b] = in_deg.get(b, 0) + 1
    q = deque([n for n, d in in_deg.items() if d == 0])
    order = []
    while q:
        u = q.popleft()
        order.append(u)
        for v in edges_map.get(u, []):
            in_deg[v] -= 1
            if in_deg[v] == 0:
                q.append(v)
    return (len(order) == len(nodes), order)

def build_reachability(nodes: Set[str], edges_map: Dict[str, List[str]]) -> Dict[str, Set[str]]:
    """Compute forward reachability for each node (excluding itself)."""
    reach = {n: set() for n in nodes}
    for src in nodes:
        # BFS/DFS
        seen = set()
        dq = deque(edges_map.get(src, []))
        while dq:
            u = dq.popleft()
            if u in seen: 
                continue
            seen.add(u)
            reach[src].add(u)
            for v in edges_map.get(u, []):
                if v not in seen:
                    dq.append(v)
    return reach

def collect_field_universe(nodes_yaml) -> Tuple[Dict[str, Set[str]], Dict[str, str], Set[str]]:
    """Return produces map, producer_of field map, and universe set."""
    nodes = nodes_yaml.get("nodes", {})
    produces_map: Dict[str, Set[str]] = {}
    producer_of: Dict[str, str] = {}
    universe: Set[str] = set()
    errs = []
    for node, spec in nodes.items():
        p = set(spec.get("produces", []) or [])
        produces_map[node] = p
        for f in p:
            if f in producer_of:
                errs.append(f"❌ Field '{f}' produced by multiple nodes: {producer_of[f]} and {node}")
            producer_of[f] = node
        universe |= p
    if errs:
        print("\n".join(errs))
        sys.exit(1)
    return produces_map, producer_of, universe

def ensure_nodes_exist(nodes_yaml, edges_map, aggregate_nodes, context="node_graph"):
    nodes_defined = set(nodes_yaml.get("nodes", {}).keys())
    missing = []
    for a, outs in edges_map.items():
        if a not in nodes_defined:
            missing.append(f"❌ {context}: node '{a}' not found in nodes.yaml")
        for b in outs:
            if b not in nodes_defined:
                missing.append(f"❌ {context}: node '{b}' not found in nodes.yaml")
    for ag in aggregate_nodes:
        if ag not in nodes_defined:
            missing.append(f"❌ aggregate_nodes: '{ag}' not found in nodes.yaml")
    return nodes_defined, missing

def normalize_rules_clears(rollback_rules_yaml) -> Tuple[Dict[str, Set[str]], Set[str]]:
    reports_fixed = set(rollback_rules_yaml.get("reports_fixed", []) or [])
    rmap = {}
    for node, spec in (rollback_rules_yaml.get("rules", {}) or {}).items():
        # accept 'clears' or legacy 'reset_set'
        clears = spec.get("clears", None)
        if clears is None:
            clears = spec.get("reset_set", [])
        rmap[node] = set(clears or [])
    return rmap, reports_fixed

def validate_rollback_policies(policies_yaml) -> Tuple[Dict[str, dict], List[str], List[str]]:
    errors, warnings = [], []
    pol_map = policies_yaml.get("policies", {})
    for name in pol_map.keys():
        if name not in ALLOWED_POLICIES:
            errors.append(f"❌ rollback_policies: policy '{name}' not in allowed {sorted(ALLOWED_POLICIES)}")
    # If they used top-level 'policy' schema (older), also validate
    if "policy" in policies_yaml and isinstance(policies_yaml["policy"], dict):
        defs = policies_yaml["policy"].get("definitions", {})
        for name in defs.keys():
            if name not in ALLOWED_POLICIES:
                errors.append(f"❌ rollback_policies.policy.definitions: policy '{name}' not in allowed {sorted(ALLOWED_POLICIES)}")
    # aggregate_nodes presence
    agg = policies_yaml.get("aggregate_nodes", policies_yaml.get("aggregate_nodes".lower(), [])) or policies_yaml.get("aggregate_nodes", [])
    if not agg:
        warnings.append("⚠️ rollback_policies: 'aggregate_nodes' not found; aggregate_only checks will be shallow.")
    return pol_map, errors, warnings

def compute_full_downstream_required(node: str,
                                     reach: Dict[str, Set[str]],
                                     produces_map: Dict[str, Set[str]],
                                     reports_fixed: Set[str]) -> Set[str]:
    s = set(produces_map.get(node, set()))
    for d in reach.get(node, set()):
        s |= produces_map.get(d, set())
    s |= reports_fixed
    return s

def compute_aggregate_only_required(node: str,
                                    reach: Dict[str, Set[str]],
                                    produces_map: Dict[str, Set[str]],
                                    aggregate_nodes: Set[str],
                                    reports_fixed: Set[str]) -> Set[str]:
    s = set(produces_map.get(node, set()))  # always re-do X itself
    for d in reach.get(node, set()):
        if d in aggregate_nodes:
            s |= produces_map.get(d, set())
    s |= reports_fixed
    return s

def main():
    ap = argparse.ArgumentParser(description="Validate nodes & rules coherence")
    ap.add_argument("--nodes", default="mappings/nodes.yaml")
    ap.add_argument("--graph", default="rules/node_graph.yaml")
    ap.add_argument("--rules", default="rules/rollback_rules.yaml")
    ap.add_argument("--policies", default="rules/rollback_policies.yaml")
    ap.add_argument("--out", default="out", help="output dir for coverage CSV")
    args = ap.parse_args()

    # Load YAMLs
    nodes_yaml = load_yaml(args.nodes)
    graph_yaml = load_yaml(args.graph)
    rules_yaml = load_yaml(args.rules)
    policies_yaml = load_yaml(args.policies)

    # Collect produces & universe
    produces_map, producer_of, universe_fields = collect_field_universe(nodes_yaml)

    # node_graph edges
    edges_map = {}
    raw_edges = graph_yaml.get("edges", {})
    for k, v in raw_edges.items():
        edges_map[k] = list(v or [])
    aggregate_nodes = set(graph_yaml.get("aggregate_nodes", []))

    # Ensure nodes exist + DAG
    nodes_defined, missing = ensure_nodes_exist(nodes_yaml, edges_map, aggregate_nodes, context="node_graph")
    # Acyclic
    ok_dag, topo = topo_check_acyclic(nodes_defined, edges_map)

    # Normalize rollback_rules
    clears_map, reports_fixed = normalize_rules_clears(rules_yaml)

    # Validate rollback_policies
    pol_map, pol_errors, pol_warnings = validate_rollback_policies(policies_yaml)
    # get aggregate_nodes from policies if provided there
    agg_from_policies = set(policies_yaml.get("aggregate_nodes", []))
    if agg_from_policies:
        aggregate_nodes |= agg_from_policies

    # REACHABILITY
    reach = build_reachability(nodes_defined, edges_map)

    # --- Structural checks report ---
    print("== Structural checks ==")
    err_count = 0
    if missing:
        err_count += len(missing)
        for m in missing:
            print(m)
    if not ok_dag:
        err_count += 1
        print("❌ node_graph contains cycles (topo sort failed).")
    else:
        print("✅ node_graph is acyclic (topological order length =", len(topo), ")")
    # rollback_rules nodes exist
    unknown_rule_nodes = [n for n in clears_map.keys() if n not in nodes_defined]
    if unknown_rule_nodes:
        err_count += len(unknown_rule_nodes)
        for n in unknown_rule_nodes:
            print(f"❌ rollback_rules: node '{n}' not found in nodes.yaml")
    # clears subset of universe? (they said: reset_set ⊆ nodes.yaml 该节点的字段全集。
    # 我们放宽为: 每个 clears 字段必须属于 universe (some node produces it) or reports_fixed)
    for n, clears in clears_map.items():
        unknown_fields = [f for f in clears if (f not in universe_fields and f not in reports_fixed)]
        if unknown_fields:
            err_count += 1
            print(f"❌ rollback_rules[{n}]: unknown fields not produced by any node or reports_fixed: {unknown_fields}")
    # policies allowed names
    if pol_errors:
        err_count += len(pol_errors)
        for e in pol_errors:
            print(e)
    if pol_warnings:
        for w in pol_warnings:
            print(w)
    if err_count == 0:
        print("✅ Structural checks passed.")

    # --- Semantic checks ---
    print("\n== Semantic checks ==")
    sem_err = 0

    # (a) No write conflicts along graph: for any reachable pair (A,B), intersect produces empty
    for a in nodes_defined:
        pa = produces_map.get(a, set())
        for b in reach.get(a, set()):
            pb = produces_map.get(b, set())
            inter = pa & pb
            if inter:
                sem_err += 1
                print(f"❌ Write conflict: fields {sorted(inter)} produced by both '{a}' and downstream '{b}'")
    if sem_err == 0:
        print("✅ No write conflicts along graph (unique ownership holds along all paths).")

    # (b) full_downstream coverage: clears[N] must superset required
    missing_full = {}
    for n in nodes_defined:
        required = compute_full_downstream_required(n, reach, produces_map, reports_fixed)
        provided = clears_map.get(n, set())
        miss = required - provided
        if miss:
            missing_full[n] = sorted(miss)
    if missing_full:
        sem_err += 1
        print("❌ full_downstream gaps: clears[] missing required fields:")
        for n, miss in missing_full.items():
            print(f"   - {n}: {miss}")
    else:
        print("✅ full_downstream coverage OK for all nodes.")

    # (c) aggregate_only must not include non-aggregate nodes (except node itself)
    if not aggregate_nodes:
        print("⚠️ aggregate_only semantic check skipped (no aggregate_nodes specified).")
    else:
        bad_aggr = {}
        for n in nodes_defined:
            required_ag = compute_aggregate_only_required(n, reach, produces_map, aggregate_nodes, reports_fixed)
            # simulate fields that come from non-aggregate nodes other than n
            non_ag_fields = set()
            # find all downstream nodes reachable that are not aggregate
            non_ag_nodes = [d for d in reach.get(n, set()) if d not in aggregate_nodes]
            for na in non_ag_nodes:
                non_ag_fields |= produces_map.get(na, set())
            # Intersection indicates leak
            leak = required_ag & non_ag_fields
            if leak:
                bad_aggr[n] = sorted(leak)
        if bad_aggr:
            sem_err += 1
            print("❌ aggregate_only leakage: following nodes would clear non-aggregate fields:")
            for n, leak in bad_aggr.items():
                print(f"   - {n}: {leak}")
        else:
            print("✅ aggregate_only will not clear non-aggregate nodes (except the node itself).")

    # --- Coverage report ---
    print("\n== Coverage report ==")
    # Full-downstream union over all N
    union_full = set()
    for n in nodes_defined:
        union_full |= compute_full_downstream_required(n, reach, produces_map, reports_fixed)

    # Aggregate-only union over all N
    union_aggr = set()
    for n in nodes_defined:
        union_aggr |= compute_aggregate_only_required(n, reach, produces_map, aggregate_nodes, reports_fixed)

    all_fields = set(universe_fields) | set(reports_fixed)
    # policies 'custom' and 'diagnostic_tiers' are caller/workflow driven; we just flag as N/A columns
    not_covered_any = all_fields - (union_full | union_aggr)
    print(f"Fields total: {len(all_fields)}")
    print(f"Covered by FULL (union over all N): {len(union_full)}")
    print(f"Covered by AGGREGATE_ONLY (union over all N): {len(union_aggr)}")
    if not_covered_any:
        print(f"❌ Uncovered fields by any policy union: {sorted(not_covered_any)}")
    else:
        print("✅ Every field is covered by at least one policy union (full or aggregate_only).")

    # Emit CSV
    out_dir = args.out
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, "coverage_report.csv")
    # columns: field, producer_node, covered_full(Y/N), covered_aggregate_only(Y/N)
    inv_producer = {f: n for n, ps in produces_map.items() for f in ps}
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("field,producer_node,covered_full,covered_aggregate_only\n")
        for fld in sorted(all_fields):
            prod = inv_producer.get(fld, "reports_fixed")
            cf = "Y" if fld in union_full else "N"
            ca = "Y" if fld in union_aggr else "N"
            f.write(f"{fld},{prod},{cf},{ca}\n")
    print(f"📝 Coverage CSV written to: {csv_path}")

    # Summary status
    print("\n== Summary ==")
    if err_count == 0 and sem_err == 0 and not not_covered_any:
        print("✅ All checks passed.")
        sys.exit(0)
    else:
        print("⚠️ Some checks failed. See logs above.")
        sys.exit(2)

if __name__ == "__main__":
    main()
