import re

with open('logs-put-sell/qc_logout_all.txt', 'r') as f:
    for i, line in enumerate(f):
        line = line.strip()
        if not (line.startswith('[+]') or line.startswith('[-]')):
            continue
        # Show what the regex actually matches
        all_pnl = re.findall(r'(\w*PnL)=\$([+\-]?[\d,]+)', line)
        first_match = re.search(r'PnL=\$([+\-]?[\d,]+)', line)
        print(f"All PnL fields: {all_pnl}")
        print(f"First regex match: {first_match.group(0) if first_match else 'none'}")
        print()
        if i > 8:
            break
