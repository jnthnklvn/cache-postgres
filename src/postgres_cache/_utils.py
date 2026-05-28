"""
Utility functions for postgres-cache.
"""

import re
from datetime import timedelta

def parse_duration(duration_str: str) -> timedelta:
    """Parse a string duration (e.g. '10m', '2h30s') into a timedelta.
    
    Supported units:
      - 'd': days
      - 'h': hours
      - 'm': minutes
      - 's': seconds
      
    Args:
        duration_str: The string representation of the duration.
        
    Returns:
        A timedelta object.
        
    Raises:
        ValueError: If the duration string is incorrectly formatted.
    """
    if not duration_str:
        raise ValueError("Empty duration string")
        
    if not re.fullmatch(r'(\d+[dhms])+', duration_str):
        raise ValueError(f"Invalid duration format: {duration_str}")
        
    parts = re.findall(r'(\d+)([dhms])', duration_str)
    kwargs = {}
    for value, unit in parts:
        if unit == 'd':
            kwargs['days'] = kwargs.get('days', 0) + int(value)
        elif unit == 'h':
            kwargs['hours'] = kwargs.get('hours', 0) + int(value)
        elif unit == 'm':
            kwargs['minutes'] = kwargs.get('minutes', 0) + int(value)
        elif unit == 's':
            kwargs['seconds'] = kwargs.get('seconds', 0) + int(value)
            
    return timedelta(**kwargs)
