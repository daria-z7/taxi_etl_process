import os
from ftplib import FTP_TLS

from dotenv import load_dotenv


load_dotenv('.env')

def create_ftps():
    """Создание подключение к FTP."""
    ftps = FTP_TLS(os.getenv('FTP_TLS_HOST'))
    ftps.login(os.getenv('FTPS_USER'), os.getenv('FTPS_PASSWORD'))
    ftps.prot_p()
    return ftps
