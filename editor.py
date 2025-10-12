from pathlib import Path
import json
from datetime import datetime

# --- Config ---
STATEMENTS_FILE = Path("data/customer_data/1234567890/statements.json")  # ← update accountId here

def add_period_field(file_path: Path):
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Load JSON
    with open(file_path, "r", encoding="utf-8") as f:
        statements = json.load(f)

    if not isinstance(statements, list):
        raise ValueError("Expected statements.json to contain a list of statement objects")

    updated_count = 0

    # Process each statement
    for stmt in statements:
        opening_date_str = stmt.get("openingDateTime")
        if not opening_date_str:
            continue

        try:
            # Try parsing date automatically (ISO-8601 or similar)
            dt = datetime.fromisoformat(opening_date_str.replace("Z", ""))
            stmt["period"] = dt.strftime("%Y-%m")  # e.g. "2025-03"
            updated_count += 1
        except ValueError:
            print(f"⚠️ Skipping invalid date: {opening_date_str}")

    # Save updated JSON back
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(statements, f, indent=2, ensure_ascii=False)

    print(f"✅ Added 'period' field to {updated_count} statements in {file_path}")

if __name__ == "__main__":
    add_period_field(STATEMENTS_FILE)
