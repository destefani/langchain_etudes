import os
import requests


class DatasetDownloader:

    ASSETS = {
        'narcotrafico_chile_2022': 'http://www.fiscaliadechile.cl/Fiscalia/quienes/Informe_2022_Observatorio_del_Narcotrafico.pdf',
    }

    def __init__(self, local_dir='../data/raw/'):
        self.local_dir = local_dir
        if not os.path.exists(self.local_dir):
            os.makedirs(self.local_dir)

    def download(self, asset_name):
        if asset_name not in self.ASSETS:
            raise ValueError(f"Asset '{asset_name}' not found.")
        
        local_file_path = os.path.join(self.local_dir, asset_name)
        
        # Check if the file has already been downloaded
        if os.path.exists(local_file_path):
            print(f"{asset_name} already exists locally.")
            return local_file_path
        
        response = requests.get(self.ASSETS[asset_name], stream=True)
        with open(local_file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
        
        print(f"{asset_name} downloaded successfully.")
        return local_file_path

    def get_local_path(self, asset_name):
        local_file_path = os.path.join(self.local_dir, asset_name)
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Asset '{asset_name}' not found locally. Use download() method first.")
        return local_file_path