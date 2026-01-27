import os
from collections import deque
from typing import Iterator, Tuple

def bfs_traverse(root: str, base_path: str) -> Iterator[Tuple[str, bool, int]]:
	if not os.path.isdir(root):
		raise ValueError("root must be an existing directory")
	queue = deque([(root, 0)])
	while queue:
		current, level = queue.popleft()
		yield (current, True, level)
		with os.scandir(os.path.join(base_path, current)) as it:
			dirs = []
			files = []
			for entry in it:
				name = entry.name
				path = entry.path.replace(base_path + os.sep, "")
				if entry.is_dir(follow_symlinks=False):
					dirs.append((name, path))
				else:
					files.append((name, path))
			dirs.sort(key=lambda x: x[0].lower())
			files.sort(key=lambda x: x[0].lower())
			for _, dpath in dirs:
				queue.append((dpath, level + 1))
			for fname, fpath in files:
				yield (fpath, False, level + 1)

def get_folders_and_files(root: str, base_path: str):
    items = [item for item in bfs_traverse(root, base_path)]
    folder_list = [{"path":path, "level": level} for path, is_dir, level in items if is_dir]
    file_list = [{"path":path, "level": level} for path, is_dir, level in items if not is_dir]
    return folder_list, file_list

def main() -> None:
    root = "files"
    base_path = os.path.dirname(__file__)
    folder_list, file_list = get_folders_and_files(root, base_path)
    for item in folder_list:
        print(item)
    for item in file_list:
        print(item)


if __name__ == "__main__":
	main()
