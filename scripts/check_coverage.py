#!/usr/bin/env python3
"""Assert per-module coverage thresholds from a Cobertura XML report."""
import sys, argparse, xml.etree.ElementTree as ET

def get_class_level_coverage(root, module):
    total_lines = 0
    covered_lines = 0
    matched_files = []
    
    for clazz in root.findall(".//class"):
        filename = clazz.get("filename", "")
        normalized_path = filename.replace("\\", "/")
        path_parts = normalized_path.split("/")
        
        # Match if module name is one of the directories or matches the file name/path
        if module in path_parts or any(module in part for part in path_parts):
            matched_files.append(filename)
            for line in clazz.findall(".//line"):
                total_lines += 1
                if int(line.get("hits", 0)) > 0:
                    covered_lines += 1
                    
    if total_lines == 0:
        return None
    pct = (covered_lines / total_lines) * 100
    return pct, total_lines, covered_lines, matched_files

def parse(root):
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

    tree = ET.parse(args.xml)
    root = tree.getroot()
    cov = parse(root)
    
    print("\n=== Workspace Crate Coverage ===")
    for k, v in sorted(cov.items()):
        print(f"  {k}: {v:.1f}%")

    failures = []
    for req in args.require:
        module, threshold = req.split("=")
        threshold = float(threshold)
        
        # Try class-level (directory/file level) matching first
        class_cov = get_class_level_coverage(root, module)
        if class_cov is not None:
            actual, total, covered, files = class_cov
            icon = "✅" if actual >= threshold else "❌"
            print(f"\n  {icon} Module '{module}': {actual:.1f}% (need {threshold}%)")
            print(f"    Lines: {covered}/{total} across {len(files)} files")
            if actual < threshold:
                failures.append(f"Module '{module}': {actual:.1f}% < {threshold}%")
        else:
            # Fallback to package matching
            matches = {k: v for k, v in cov.items() if module in k}
            if not matches:
                # Fallback to overall
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
