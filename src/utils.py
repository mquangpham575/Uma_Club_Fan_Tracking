import ctypes
import os
import sys


def setup_windows_console():
    """Disables QuickEdit Mode in Windows Console to prevent freezing on click."""
    if sys.platform == 'win32':
        try:
            kernel32 = ctypes.windll.kernel32
            mode = ctypes.c_ulong()
            handle = kernel32.GetStdHandle(-10) # STD_INPUT_HANDLE
            
            kernel32.GetConsoleMode(handle, ctypes.byref(mode))
            # Disable ENABLE_QUICK_EDIT_MODE (0x0040) and ENABLE_INSERT_MODE (0x0020)
            mode.value &= ~0x0060 
            kernel32.SetConsoleMode(handle, mode)
        except Exception:
            pass

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')
