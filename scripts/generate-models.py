import os
import sys
import subprocess
from dotenv import load_dotenv

load_dotenv()

tables = [
    "assets",
    "providers_resolutions",
    "providers",
    "resolutions",
]

cmd = [
    sys.executable,
    "-m",
    "sqlacodegen",
    "--generator", "foreign_key_id_suffix",
    "--options", "use_inflect",
    "--tables", ",".join(tables),
    "--outfile", "bafrapy/models/generated.py",
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_EXTERNAL_PORT')}/"
    f"{os.getenv('DB_DATABASE')}?charset=utf8mb4",
]

subprocess.run(cmd, check=True)
