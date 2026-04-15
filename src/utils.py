import ctypes
import os
import sys



def setup_windows_console(version_str=None):
    """
    Disables QuickEdit Mode and enables ANSI color support.
    Sets Console Title if version is provided.
    """
    if sys.platform == 'win32':
        try:
            kernel32 = ctypes.windll.kernel32
            
            # Set Title
            if version_str:
                kernel32.SetConsoleTitleW(f"Endless_{version_str}")
            
            # Disable QuickEdit
            mode = ctypes.c_ulong()
            handle = kernel32.GetStdHandle(-10) # STD_INPUT_HANDLE
            kernel32.GetConsoleMode(handle, ctypes.byref(mode))
            mode.value &= ~0x0060 
            kernel32.SetConsoleMode(handle, mode)

            # Enable Virtual Terminal Processing (ANSI colors)
            h_out = kernel32.GetStdHandle(-11) # STD_OUTPUT_HANDLE
            out_mode = ctypes.c_ulong()
            if kernel32.GetConsoleMode(h_out, ctypes.byref(out_mode)):
                kernel32.SetConsoleMode(h_out, out_mode.value | 0x0004)
        except Exception:
            pass

class LogColor:
    """ANSI color codes for consistent logging."""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    SUCCESS = "\033[92m"  # Light Green
    COOLDOWN = "\033[96m" # Cyan
    SCRAPER = "\033[95m"  # Magenta
    API = "\033[94m"      # Light Blue
    RETRY = "\033[93m"    # Light Yellow
    ERROR = "\033[91m"    # Light Red
    BATCH = "\033[90m"    # Gray

def colorize(text: str, color: str) -> str:
    """Wraps text with ANSI color codes."""
    return f"{color}{text}{LogColor.RESET}"

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')
