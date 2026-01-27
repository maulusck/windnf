# logger.py
import logging
import sys
from typing import Union


def is_dumb_terminal() -> bool:
    return not sys.stdout.isatty()


class Colors:
    # Reset
    RESET = "\033[0m"

    # Styles
    BOLD = "\033[1m"
    DIM = "\033[2m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    REVERSE = "\033[7m"
    HIDDEN = "\033[8m"

    # Foreground colors
    FG_BLACK = "\033[30m"
    FG_RED = "\033[31m"
    FG_GREEN = "\033[32m"
    FG_YELLOW = "\033[33m"
    FG_BLUE = "\033[34m"
    FG_MAGENTA = "\033[35m"
    FG_CYAN = "\033[36m"
    FG_WHITE = "\033[37m"

    FG_BRIGHT_BLACK = "\033[90m"
    FG_BRIGHT_RED = "\033[91m"
    FG_BRIGHT_GREEN = "\033[92m"
    FG_BRIGHT_YELLOW = "\033[93m"
    FG_BRIGHT_BLUE = "\033[94m"
    FG_BRIGHT_MAGENTA = "\033[95m"
    FG_BRIGHT_CYAN = "\033[96m"
    FG_BRIGHT_WHITE = "\033[97m"

    # Background colors
    BG_BLACK = "\033[40m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_WHITE = "\033[47m"

    BG_BRIGHT_BLACK = "\033[100m"
    BG_BRIGHT_RED = "\033[101m"
    BG_BRIGHT_GREEN = "\033[102m"
    BG_BRIGHT_YELLOW = "\033[103m"
    BG_BRIGHT_BLUE = "\033[104m"
    BG_BRIGHT_MAGENTA = "\033[105m"
    BG_BRIGHT_CYAN = "\033[106m"
    BG_BRIGHT_WHITE = "\033[107m"


# -------------------------------------------------
# Disable colors on dumb terminals (single side-effect)
# -------------------------------------------------
if is_dumb_terminal():
    for name, value in vars(Colors).items():
        if isinstance(value, str) and value.startswith("\033"):
            setattr(Colors, name, "")


class ColorFormatter(logging.Formatter):
    COLOR_MAP = {
        logging.DEBUG: Colors.FG_CYAN,
        logging.INFO: Colors.FG_GREEN,
        logging.WARNING: Colors.FG_YELLOW,
        logging.ERROR: Colors.FG_RED,
        logging.CRITICAL: Colors.FG_RED + Colors.BOLD,
    }

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLOR_MAP.get(record.levelno, "")
        message = super().format(record)
        return f"{color}{message}{Colors.RESET}"


_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}


def setup_logger(
    name: str = "windnf",
    level: Union[int, str] = "info",
) -> logging.Logger:
    """
    Configure the application logger.

    Must be called exactly once by the CLI / entrypoint.
    All other modules should use logging.getLogger(__name__).
    """
    if isinstance(level, str):
        level = _LEVELS.get(level.lower(), logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    # Idempotent: do not add handlers twice
    if logger.handlers:
        return logger

    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(ColorFormatter("%(message)s"))

    logger.addHandler(handler)
    return logger
