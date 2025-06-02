# src/common/utils/filename_parser.py

import re
from datetime import datetime

class FileNameParser:
    """
    Parser for expected filename patterns:
    e.g. AAA_BBB_20250501_W_20250502120515517.csv.ZIP

    Attributes extracted:
      - prefix: 'AAA_BBB'
      - extract_date: date of extraction as datetime.date
      - file_type: 'W' or 'P'
      - file_timestamp: actual timestamp embedded in filename
    """
    PATTERN = re.compile(
        r"^(?P<prefix>[A-Z0-9_]+)_(?P<extract_date>\d{8})_(?P<type>[WP])_(?P<file_ts>\d{17,18})\.csv\.ZIP$"
    )

    @classmethod
    def parse(cls, filename: str) -> dict:
        """
        Parse filename and return dict with keys:
          prefix, extract_date, file_type, file_timestamp

        Raises ValueError if format is invalid.
        """
        base = filename.split('/')[-1]
        m = cls.PATTERN.match(base)
        if not m:
            raise ValueError(f"Filename does not match expected pattern: {filename}")

        gd = m.groupdict()
        try:
            extract_date = datetime.strptime(gd['extract_date'], '%Y%m%d').date()
            file_ts = datetime.strptime(gd['file_ts'], '%Y%m%d%H%M%S')
        except Exception as e:
            raise ValueError(f"Error parsing dates from filename {filename}: {e}")

        return {
            'prefix': gd['prefix'],
            'extract_date': extract_date,
            'file_type': gd['type'],
            'file_timestamp': file_ts,
        }
