import click
import pandas as pd  # type: ignore


@click.command("excel")
@click.argument("file", type=click.Path(exists=True))
def parse_excel(file: str):
    excel_file = pd.ExcelFile(file)
    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(file, sheet_name=sheet_name)
        print(f"Sheet: {sheet_name}")
        print(df.head())
        print("\n" + "-" * 100 + "\n")


if __name__ == "__main__":
    parse_excel()
