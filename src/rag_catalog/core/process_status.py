"""Non-destructive process probes shared by recovery and monitoring code."""

import psutil


def process_is_alive(pid: int) -> bool:
    try:
        value = int(pid or 0)
    except (ValueError, TypeError):
        return False
    # os.kill(pid, 0) terminates the target on Windows.
    return value > 0 and psutil.pid_exists(value)
