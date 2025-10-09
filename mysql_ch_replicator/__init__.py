import importlib.metadata

from .main import main

try:
    __version__ = importlib.metadata.version("mysql-ch-replicator")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"  # fallback version
