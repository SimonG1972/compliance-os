# scripts/validate_policies.py
import glob, yaml, sys, os
ALLOWED_TOP = {
  "host","aliases","normalization","discovery","hydration",
  "cleaning","chunking","tagging","backoff","version"
}
def main():
    errs = 0
    for f in glob.glob(os.path.join("config","policies","*.yml")):
        with open(f,"r",encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        for k in data.keys():
            if k not in ALLOWED_TOP:
                print(f"[warn] {f}: unknown top-level key '{k}'")
        if "host" not in data:
            print(f"[err] {f}: missing 'host'")
            errs += 1
    if errs:
        sys.exit(1)
    print("Policies look OK.")
if __name__ == "__main__":
    main()
