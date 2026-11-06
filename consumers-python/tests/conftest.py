import os
import sys

# common.config refuses to import without Postgres settings; tests never connect.
os.environ.setdefault("POSTGRES_DB", "test")
os.environ.setdefault("POSTGRES_USER", "test")
os.environ.setdefault("POSTGRES_PASSWORD", "test")

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
