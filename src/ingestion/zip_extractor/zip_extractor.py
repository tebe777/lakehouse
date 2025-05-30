import zipfile
from pathlib import Path
from typing import List

class ZipExtractor:
    """"""Extracts ZIP files and processes CSV contents""""""
    
    def extract(self, zip_path: str, extract_to: str = None) -> List[str]:
        """"""Extract ZIP file and return list of extracted CSV files""""""
        if extract_to is None:
            extract_to = Path(zip_path).parent / "extracted"
        
        extracted_files = []
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
                extracted_files = [
                    str(Path(extract_to) / file) 
                    for file in zip_ref.namelist() 
                    if file.endswith('.csv')
                ]
        except Exception as e:
            raise Exception(f"Failed to extract ZIP file {zip_path}: {str(e)}")
        
        return extracted_files