import importlib.metadata
import os

os.environ['TZ'] = 'UTC'
try:
    import time
    time.tzset()
except AttributeError:
    pass

from .main import main

try:
    __version__ = importlib.metadata.version("mysql-ch-replicator")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"  # fallback version
