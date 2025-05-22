import functools
import subprocess
from typing import TextIO

import tomllib

run = functools.partial(subprocess.run, shell=True)


def build_doc(version: str) -> None:
    print(f"Build _{version}_")
    run(f"git checkout v{version}")
    run("git checkout main -- conf.py")
    run("git checkout main -- meta.toml")

    run("sphinx-build -M html . _build")
    run(f"mv _build/html pages/{version}")
    run("rm -rf _build")
    run("git checkout main")


def build_init_page(version: str) -> None:
    f: TextIO
    with open("pages/index.html", "w") as f:
        f.write(f"""<!doctype html>
<meta http-equiv="refresh" content="0; url=./{version}/">""")


if __name__ == "__main__":
    # Executes in the docs directory, hence the constant use of ..
    # create pages folder
    run("rm -rf pages")
    run("mkdir pages")

    # get versions
    with open("meta.toml", "rb") as f:
        versions = tomllib.load(f)["versions"]
    versions.sort(reverse=True, key=lambda v: tuple(map(int, v.split("."))))
    print(f"All detected versions: {versions}")

    # build documentations for different versions
    for version in versions:
        build_doc(version)

    # build initial page that redirect to the latest version
    build_init_page(versions[0])
