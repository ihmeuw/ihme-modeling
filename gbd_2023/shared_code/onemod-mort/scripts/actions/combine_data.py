import fire
import pandas as pd
from pplkit.data.interface import DataInterface
from tqdm.auto import tqdm


def main(directory: str, dirpath: str) -> None:
    dataif = DataInterface(directory=directory)
    dataif.add_dir("target", dataif.directory / dirpath)

    if dataif.target.exists():
        data = []
        file_paths = list(dataif.target.iterdir())
        print(f"There are {len(file_paths)} files to combine")
        for file_path in tqdm(file_paths):
            data.append(dataif.load(file_path))

        data = pd.concat(data, axis=0).reset_index(drop=True)
        dataif.dump(
            data, dataif.target.parent / f"{dataif.target.name}.parquet"
        )


if __name__ == "__main__":
    fire.Fire(main)
