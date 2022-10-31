##  taxi_etl_process

### Описание проекта:

Скрипт выполняет следующие действия: 
- выгружает данные из бд-источника и FTP
- загружает данные в бд-хранилище данных
- составляет 4 отчета
- выполнено логирование основных событий - файл app/main.log.

### Технологии:

При реализации проекта были использованы следующие основные технологии, фреймворки и библиотеки:
- Python 3.10
- psycopg2-binary 2.9.3
- pandas 1.5.0


### Как запустить проект:
Клонируйте репозиторий и перейдите в него в командной строке:

```
git clone 'ссылка на репозиторий'
```

```
cd taxi_etl_process
```
Cоздайте и активируйте виртуальное окружение:

```
python -m venv venv
```

```
source venv/Scripts/activate
```

Перейдите в папку app:

```
cd app
```

```
python -m pip install --upgrade pip
```

Установите зависимости из файла requirements.txt:

```
pip install -r requirements.txt
```

В папке app cоздайте файл .env и заполните его следующими значениями:

```
FTP_TLS_HOST=de-edu-db.chronosavant.ru
FTPS_USER=etl_tech_user
FTPS_PASSWORD=# укажите пароль
SOURCE_DB_SHEMA=main
SOURCE_DB_HOST=de-edu-db.chronosavant.ru
SOURCE_DB_PORT=5432
SOURCE_DB_NAME=taxi
SOURCE_DB_USER=etl_tech_user
SOURCE_DB_PASSWORD= # укажите пароль
WH_DB_SHEMA=dwh_saint_petersburg
WH_DB_HOST=de-edu-db.chronosavant.ru
WH_DB_PORT=5432
WH_DB_NAME=dwh
WH_DB_USER=dwh_saint_petersburg
WH_DB_PASSWORD= # укажите пароль
```

Запустите процесс:

```
python main.py
```

### Авторы проекта:
- Камилла Хуранова
- Павел Вервейн
- Тимур Шагимуратов
- Никита Гирш
- Дарья Зайцева
