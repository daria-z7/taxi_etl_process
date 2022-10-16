from ftplib import FTP_TLS


ftps = FTP_TLS('de-edu-db.chronosavant.ru')
ftps.login('etl_tech_user', 'etl_tech_user_password')
 
ftps.prot_p()
ftps.retrlines('LIST')

#file_name = ''
#my_file = open(file_name, 'r')
#print(my_file)

ftps.quit()