from ingestion.delta import delta

import click


@click.group("delta")
def delta_cli():
    pass


@click.command("show")
@click.argument("path")
def read_delta(path: str):
    delta.show(path)


delta_cli.add_command(read_delta)

if __name__ == "__main__":
    delta_cli()
