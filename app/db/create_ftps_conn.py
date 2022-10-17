from ftplib import FTP_TLS


def create_ftps():
    ftps = FTP_TLS('de-edu-db.chronosavant.ru')
    ftps.login('etl_tech_user', 'etl_tech_user_password')
    ftps.prot_p()
    return ftps