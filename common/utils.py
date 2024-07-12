# Commmon functions that can be imported by easi-cookbook notebooks and scripts

from datetime import timedelta

def elapsed_time(seconds:float) -> str:
    """Format the seconds into a human readable string"""
    if isinstance(seconds, timedelta):
        seconds = timedelta.total_seconds(seconds)
    if seconds < 120:
        return f'{seconds:.2f} seconds'
    elif seconds < 3600:
        seconds = seconds / 60.
        return f'{seconds:.2f} minutes'
    else:
        seconds = seconds / 3600.
        return f'{seconds:.2f} hours'
