#!/usr/bin/env python3
"""Assert per-module coverage thresholds from a Cobertura XML report."""
import sys, argparse, xml.etree.ElementTree as ET

def parse(xml_path):
    root = ET.parse(xml_path).getroot()
    result = {"overall": float(root.get("line-rate", 0)) * 100}
    for pkg in root.findall(".//package"):
        name = pkg.get("name", "").replace("/", "::").replace("\\", "::")
        result[name] = float(pkg.get("line-rate", 0)) * 100
    return result

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--xml", required=True)
    p.add_argument("--require", action="append", default=[], metavar="MODULE=PCT")
    args = p.parse_args()

    cov = parse(args.xml)
    print("\n=== Coverage ===")
    for k, v in sorted(cov.items()):
        print(f"  {k}: {v:.1f}%")

    failures = []
    for req in args.require:
        module, threshold = req.split("=")
        threshold = float(threshold)
        matches = {k: v for k, v in cov.items() if module in k}
        if not matches:
            matches = {"overall": cov.get("overall", 0)}
        for pkg, actual in matches.items():
            icon = "✅" if actual >= threshold else "❌"
            print(f"\n  {icon} {pkg}: {actual:.1f}% (need {threshold}%)")
            if actual < threshold:
                failures.append(f"{pkg}: {actual:.1f}% < {threshold}%")

    if failures:
        print("\n❌ Coverage gates failed:")
        for f in failures: print(f"  - {f}")
        sys.exit(1)
    print("\n✅ All coverage gates passed.")

if __name__ == "__main__":
    main()
