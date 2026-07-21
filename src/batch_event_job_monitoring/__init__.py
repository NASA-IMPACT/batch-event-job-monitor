from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("batch-event-job-monitoring")
except PackageNotFoundError:
    __version__ = "unknown"
