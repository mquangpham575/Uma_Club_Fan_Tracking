import ctypes
import os
import sys


def setup_windows_console(version_str=None):
    """
    Disables QuickEdit Mode to prevent freezing.
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
            # Disable ENABLE_QUICK_EDIT_MODE (0x0040) and ENABLE_INSERT_MODE (0x0020)
            mode.value &= ~0x0060 
            kernel32.SetConsoleMode(handle, mode)
        except Exception:
            pass

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')
