import importlib.metadata
import os

# respect TZ env var passed to docker container via -e flag. If TZ flag wasn't set then fallback to UTC timezone.
tz = os.environ.get('TZ', 'UTC')
os.environ['TZ'] = tz
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
