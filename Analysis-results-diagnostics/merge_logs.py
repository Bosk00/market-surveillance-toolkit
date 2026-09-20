"""
Merges all trade log .txt files in the current folder into one clean trades_log.txt
Deduplicates by Token ID so no trade is counted twice.
Run with: py -3.11 merge_logs.py
"""
import os
import glob

OUTPUT_FILE = "trades_log.txt"

def parse_blocks(filepath):
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except:
        print(f"  Could not read {filepath}")
        return []
    return content.split("=" * 50)

def get_token_id(block):
    for line in block.strip().split("\n"):
        line = line.strip()
        if line.startswith("Token ID:"):
            tid = line.replace("Token ID:", "").strip()
            if len(tid) > 10:
                return tid
    return None

def main():
    # Find all .txt files except the output file
    txt_files = [f for f in glob.glob("*.txt") if f != OUTPUT_FILE]

    if not txt_files:
        print("No .txt files found in current folder.")
        return

    print(f"Found {len(txt_files)} file(s) to merge:")
    for f in txt_files:
        print(f"  {f}")

    seen_tokens = set()
    unique_blocks = []
    total_blocks = 0
    dupes = 0

    for filepath in txt_files:
        blocks = parse_blocks(filepath)
        for block in blocks:
            if "Time:" not in block or "Token ID:" not in block:
                continue
            total_blocks += 1
            tid = get_token_id(block)
            if tid is None:
                continue
            if tid in seen_tokens:
                dupes += 1
                continue
            seen_tokens.add(tid)
            unique_blocks.append(block.strip())

    # Sort by time
    def get_time(block):
        for line in block.split("\n"):
            if line.strip().startswith("Time:"):
                return line.replace("Time:", "").strip()
        return ""
    unique_blocks.sort(key=get_time)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for block in unique_blocks:
            f.write("\n" + "=" * 50 + "\n")
            f.write(block + "\n")

    print(f"\nDone.")
    print(f"  Total trades found:    {total_blocks}")
    print(f"  Duplicates removed:    {dupes}")
    print(f"  Unique trades written: {len(unique_blocks)}")
    print(f"  Output: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()