
import requests
import logging
import zipfile

from io import BytesIO
from typing import List


class DataLoader:
    
    def __init__(self, data_path_name: str):
        self.data_path_name = data_path_name
    
    def download_unzip_store_from_url(self, url: str) -> str:
        """
        Download a file and return response
        
        :params url: url of the stored file
        """
        req = requests.get(url)
        filename = url.split('/')[-1].strip(".zip")
        
        try:
            req = requests.get(url)
        except Exception as e:
            logging.error(f"Cannot download file {url}")
            raise e
        
        zipfile_reference = zipfile.ZipFile(BytesIO(req.content))
        zipfile_reference.extractall(self.data_path_name)
        
        logging.info(f"Finished writing to {filename}")

        return filename
    
    def download_unzip_store_from_urls(self, urls: List[str]) -> List[str]:
        filenames = []
        for url in urls:
            filename = self.download_unzip_store_from_url(url)
            filenames.append(filename)
            
        return filenames
