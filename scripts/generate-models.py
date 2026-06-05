import os
import subprocess
import sys

from dotenv import load_dotenv

load_dotenv()

exchange_tables = ["exchanges_resolutions", "exchanges", "market_availability", "markets", "resolutions", "tasks"]

GENERATION_CONFIGS = [
    {"tables": exchange_tables, "outfile": "tmp/generated/backoffice/models/exchange.py"},
]


def build_sqlacodegen_cmd(tables: list[str], outfile: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "sqlacodegen",
        "--generator",
        "declarative",
        "--options",
        "use_inflect",
        "--tables",
        ",".join(tables),
        "--outfile",
        outfile,
        f"postgresql+psycopg://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_EXTERNAL_PORT')}/"
        f"{os.getenv('DB_DATABASE')}",
    ]


def main() -> None:
    for config in GENERATION_CONFIGS:
        os.makedirs(os.path.dirname(config["outfile"]), exist_ok=True)
        subprocess.run(
            build_sqlacodegen_cmd(config["tables"], config["outfile"]),
            check=True,
        )


if __name__ == "__main__":
    main()
