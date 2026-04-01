import os
import json
from pathlib import Path

def main():
    lst = []
    local_path = os.getenv("SOURCE_LOCAL_CONTAINER_PATH")
    for f in Path(local_path).rglob("*"):
        if f.is_file():
            lst.append({
                "path": str(f.absolute()),
                "filename": f.name
            })
    with open("./filelist_20260401.json", "w", encoding="utf-8") as f:
       json.dump(lst, f)


if __name__ == "__main__":
    main()