import json, sys, pathlib
from jsonschema import validate, Draft202012Validator

def load_schema(p): 
    return json.loads(pathlib.Path(p).read_text(encoding="utf-8"))

SCHEMA = load_schema("schemas/action.schema.json")

def check_line(obj, idx):
    errors = sorted(Draft202012Validator(SCHEMA).iter_errors(obj), key=lambda e: e.path)
    return [f"[{idx}] {e.message} @path: {'/'.join(map(str,e.path))}" for e in errors]

if __name__ == "__main__":
    fn = sys.argv[1] if len(sys.argv)>1 else "data/planner/train.jsonl"
    bad = 0
    with open(fn, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            if not line.strip(): continue
            obj = json.loads(line)
            errs = check_line(obj["action"] if "action" in obj else obj, i)
            if errs:
                bad += 1
                print("\n".join(errs))
    print("OK" if bad==0 else f"Found {bad} invalid actions")
    sys.exit(1 if bad else 0)
