import click

from lsst.pipe.base.quantum_graph._convert import convert_multiblock_to_parquet


@click.command
@click.argument("path")
def run(path: str) -> None:
    convert_multiblock_to_parquet(path, "output.zip")


if __name__ == "__main__":
    run()
