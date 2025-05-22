from pathlib import Path

import fire
from pplkit.data.interface import DataInterface
from tqdm.auto import tqdm


def get_file_paths(path: str | Path) -> list[Path]:
    path = Path(path)
    if path.is_file():
        return [path]
    paths = []
    for sub_path in path.iterdir():
        paths.extend(get_file_paths(sub_path))
    return paths


def rm_tree(path: str | Path) -> None:
    path = Path(path)
    if path.is_file():
        path.unlink()
    else:
        for sub_path in path.iterdir():
            rm_tree(sub_path)
        path.rmdir()


def main(directory: str, dirpath: str) -> None:
    dataif = DataInterface(directory=directory)
    dataif.add_dir("target", dataif.directory / dirpath)

    if dataif.target.exists():
        file_paths = get_file_paths(dataif.target)
        print(f"There are {len(file_paths)} files to remove")
        for file_path in tqdm(file_paths):
            file_path.unlink()
        rm_tree(dataif.target)


if __name__ == "__main__":
    fire.Fire(main)
