import os
import logging

default_format = (
    "%(asctime)s %(filename)s:%(lineno)d [%(levelname)s] %(name)s: %(message)s"
)
abridged_format = (
    "%(asctime)s [%(levelname)s] %(message)s"
)
default_datefmt = "%Y-%m-%d %H:%M:%S"
default_log_args = {
    "level": (
        logging.DEBUG
        if os.environ.get("DEBUG", "false").lower() == "true"
        else logging.INFO
    ),
    "format": abridged_format,
    "datefmt": default_datefmt,
    "force": True,
}

if logging.getLogger().hasHandlers():
    logging.getLogger().setLevel(default_log_args["level"])
else:
    logging.basicConfig(**default_log_args)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
