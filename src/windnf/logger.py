# logger.py
import logging
import sys


def is_dumb_terminal():
    return not sys.stdout.isatty()


class Colors:
    # ANSI definitions (default = enabled)
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


# Disable all colors on dumb terminals (one place, pythonic)
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

    def format(self, record):
        color = self.COLOR_MAP.get(record.levelno, "")
        message = super().format(record)
        return f"{color}{message}{Colors.RESET}"


def setup_logger(name="windnf", level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.hasHandlers():
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(ColorFormatter("%(message)s"))
        logger.addHandler(ch)

    return logger
