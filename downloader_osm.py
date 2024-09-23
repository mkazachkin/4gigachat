import logging
import os
import subprocess
import sys

import psycopg2
import requests
import shutil

from russia_osm import osmdb_config


class OSMDownloader:
    """
    Класс для загрузки OSM-данных.

    Атрибуты:
        CONFIG: Конфигурация для подключения к базе данных OSM.
        BASE_PATH (str): Путь к корневому каталогу приложения.
        DOWNLOADS (str): Папка для загрузки данных.
        PBF_URL (str): URL для загрузки PBF-файла.
        PBF_FILENAME (str): Имя PBF-файла для загрузки.
        PBF_BACKUP (str): Имя файла для резервного копирования PBF-файла.
        TABLES_OSM (List[str]): Список таблиц БД OSM.
        LAYERS (Dict[str, str]): Словарь слоёв с соответствующими SQL-запросами для их загрузки.
    """
    CONFIG = osmdb_config
    BASE_PATH = os.path.dirname(os.path.abspath(__file__))
    DOWNLOADS = os.path.join(BASE_PATH, 'downloads')
    PBF_URL = 'https://download.geofabrik.de/russia-latest.osm.pbf'
    PBF_FILENAME = 'map.pbf'
    PBF_BACKUP = 'map.backup'
    TABLES_OSM = [
        'planet_osm_line',
        'planet_osm_point',
        'planet_osm_polygon',
        'planet_osm_roads',
    ]
    LAYERS = {
        'line_water': f'SELECT uuid_generate_v4() id, way geom, pol.waterway code, pol."name" anno '
                      f"INTO {CONFIG.SCHEMA}.line_water "
                      f"FROM {CONFIG.SCHEMA}.planet_osm_line pol "
                      f"WHERE pol.waterway IS NOT NULL;"
                      f"ALTER TABLE {CONFIG.SCHEMA}.line_water ADD CONSTRAINT line_water_pk PRIMARY KEY (id);"
                      f"CREATE INDEX line_water_code_idx ON {CONFIG.SCHEMA}.line_water (code);",
        'area_water': f'SELECT uuid_generate_v4() id, way geom, pop.natural code, pop.waterway, pop.water, '
                      f'pop."name" anno '
                      f"INTO {CONFIG.SCHEMA}.area_water "
                      f"FROM {CONFIG.SCHEMA}.planet_osm_polygon pop "
                      f"WHERE pop.natural = 'water' "
                      f"OR pop.natural = 'bay' "
                      f"OR pop.natural = 'shoal' "
                      f"OR pop.natural = 'strait' "
                      f"OR pop.natural = 'waterfall' "
                      f"OR (pop.natural IS NULL AND (pop.water IS NOT NULL OR pop.waterway IS NOT NULL));"
                      f"ALTER TABLE {CONFIG.SCHEMA}.area_water ADD CONSTRAINT area_water_pk PRIMARY KEY (id);"
                      f"CREATE INDEX area_water_code_idx ON {CONFIG.SCHEMA}.area_water (code);",
    }

    def __init__(self):
        self.conn = None
        self.cur = None

    def run(self, force_download: bool = False, force_update: bool = False, recreate_layers: bool = False):
        download_is_obsolete = self.check_if_obsolete()
        download_is_done = False
        db_updated = False
        if download_is_obsolete or force_download:
            backup_is_done = self.prepare_download()
            self.prepare_update(backup_is_done)
        elif not force_update and not recreate_layers:
            sys.exit(0)
        if download_is_done or force_update:
            db_updated = self.update_db()
        if db_updated or recreate_layers:
            self.create_layers()

    def prepare_download(self) -> bool:
        """
        Подготовка к загрузке данных.
        """
        logging.info("Проверка существования файла карты и создание резервной копии.")
        try:
            backup_is_done = self.manage_backup()
        except PermissionError:
            logging.fatal(
                f'Отказано в доступе к файлу "{self.PBF_FILENAME}". Продолжение невозможно.'
            )
            sys.exit(1)
        except OSError:
            logging.fatal(
                f'Невозможно создать резервную копию "{self.PBF_FILENAME}". Продолжение невозможно.'
            )
            sys.exit(1)
        if backup_is_done:
            logging.info("Создана резервная копия файла карты. Готов к загрузке.")
        else:
            logging.warning("Создание резервной копии не удалось. Выполняю загрузку без нее.")
        return backup_is_done

    def prepare_update(self, backup_is_done: bool):
        """
        Подготовка к обновлению данных.
        Проверяет успешность загрузки нового файла PBF и восстанавливает резервную копию, если загрузка неудачна.
        """
        download_is_done = self.download_pbf()
        if backup_is_done and download_is_done:
            logging.info("Загрузка завершена. Готов к обновлению.")
            self.delete_backup()
        elif backup_is_done and not download_is_done:
            logging.fatal("Загрузка не удалась. Восстанавливаю резервную копию и заканчиваю работу.")
            self.restore_backup()
            sys.exit(1)
        else:
            logging.fatal("Загрузка не удалась. Заканчиваю работу.")
            sys.exit(1)
        return download_is_done

    def manage_backup(self) -> bool:
        """
        Управление созданием резервной копии файла PBF.
        Если текущий файл PBF устарел, выполняется создание резервной копии.
        Возвращает True, если резервная копия успешно создана, и False, если файл PBF не был найден.
        """
        try:
            self._create_file_backup()
            return True
        except FileNotFoundError:
            return False

    def check_if_obsolete(self) -> bool:
        """
        Проверка необходимости обновления файла PBF, хранящегося в локальной директории DOWNLOADS.
        Возвращает True, если размер локального файла отличается от размера удалённого или не найден.
        В противном случае возвращает False.
        """
        try:
            st = os.stat(os.path.join(self.DOWNLOADS, self.PBF_FILENAME))
            local_file_size = st.st_size
            remote_file_size = int(requests.head(self.PBF_URL).headers["Content-Length"])
            if local_file_size == remote_file_size:
                logging.info("Обновление не требуется.")
                return False
            else:
                logging.info("Требуется обновление.")
                return True
        except FileNotFoundError:
            logging.info("Файл карты не найден. Требуется обновление.")
            return False

    def _create_file_backup(self):
        """
        Создание резервной копии файла PBF.
        """
        backup_path = os.path.join(self.DOWNLOADS, self.PBF_BACKUP)
        shutil.move(os.path.join(self.DOWNLOADS, self.PBF_FILENAME), backup_path)

    def download_pbf(self) -> bool:
        """
        Загружает PBF-файл и сохраняет его в папку downloads.
        Возвращает:
            bool: True, если загрузка прошла успешно, иначе False.
        """
        map_file = os.path.join(self.DOWNLOADS, self.PBF_FILENAME)
        try:
            with open(map_file, "wb") as file:
                with requests.get(self.PBF_URL, stream=True) as response:
                    total_size = int(response.headers['Content-Length'])
                    downloaded_size = 0
                    percent_complete = 0
                    last_percent_report = 0
                    while downloaded_size < total_size:
                        chunk = response.raw.read(1024 * 1024)
                        if not chunk:
                            break
                        downloaded_size += len(chunk)
                        file.write(chunk)
                        percent_complete = downloaded_size * 100 / total_size
                        if percent_complete - last_percent_report >= 5:
                            last_percent_report = percent_complete
                            logging.info(f"\r{percent_complete:.2f}% загружено...")
        except Exception as err:
            logging.fatal(f"Не удалось загрузить PBF-файл: {err}")
            return False
        return True

    def connect(self):
        """
        Подключение к базе данных.
        Примечания:
            Вызывает исключение и завершает программу, если соединение не удается.
        """
        try:
            self.conn = psycopg2.connect(
                dbname=self.CONFIG.DATABASE_NAME,
                user=self.CONFIG.USERNAME,
                password=self.CONFIG.PASSWORD,
                host=self.CONFIG.HOST,
                port=self.CONFIG.PORT
            )
            self.cur = self.conn.cursor()
            logging.info("Соединение с базой данных установлено.")
        except Exception as err:
            logging.fatal(f"Не удалось подключиться к базе данных: {err}")
            sys.exit(1)

    def disconnect(self):
        """
        Закрытие текущего соединения с базой данных, если оно открыто.
        """
        if self.conn is not None:
            self.cur.close()
            self.conn.close()
            logging.info("Соединение с базой данных закрыто.")

    def restore_backup(self):
        """
        Восстанавливает файл из резервной копии.
        """
        try:
            backup_path = os.path.join(self.DOWNLOADS, self.PBF_BACKUP)
            shutil.move(backup_path, os.path.join(self.DOWNLOADS, self.PBF_FILENAME))
            logging.info(f"Восстановлен файл из резервной копии: {self.PBF_FILENAME}.")
        except Exception as err:
            logging.warning(f"Не удалось восстановить файл из резервной копии: {err}")

    def delete_backup(self):
        """
           Удаляет резервную копию файла.
           Примечания:
               - Проверяет существование резервной копии.
               - Удаляет резервную копию, если она существует.
               - Выводит информацию об удалении резервной копии.
           """
        try:
            backup_path = os.path.join(self.DOWNLOADS, self.PBF_BACKUP)
            if os.path.exists(backup_path):
                os.remove(backup_path)
                logging.info(f"Резервная копия удалена: {self.PBF_FILENAME}_backup")
        except Exception as err:
            logging.warning(f"Не удалось удалить резервную копию: {err}")

    def update_db(self):
        """
        Обновляет базу данных.
        """
        backup_is_done = self.backup_tables(self.TABLES_OSM)
        self.drop_tables(self.TABLES_OSM)
        update_complete = self.update_osm_tables()
        if update_complete and backup_is_done:
            self.drop_tables([f'{t}_backup' for t in self.TABLES_OSM])
        elif not update_complete:
            if backup_is_done:
                if self.restore_tables(self.TABLES_OSM):
                    self.drop_tables([f'{t}_backup' for t in self.TABLES_OSM])
                    logging.warning("Восстановлены резервные копии таблиц.")
            logging.fatal("Не удалось обновить базу данных.")
            self.disconnect()
            sys.exit(1)
        logging.info("Обновление БД OSM завершено.")
        return update_complete

    def _copy_table(self, orig_table: str, dest_table: str):
        """
        Создает копию таблицы в БД.
        Аргументы:
            orig_table:str - имя исходной таблицы.
            dest_table:str - имя целевой таблицы.
        """
        query = f"DROP TABLE IF EXISTS {self.CONFIG.SCHEMA}.{dest_table};" \
                f"CREATE TABLE {self.CONFIG.SCHEMA}.{dest_table} (LIKE {self.CONFIG.SCHEMA}.{orig_table} "\
                f"INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);" \
                f"INSERT INTO {self.CONFIG.SCHEMA}.{dest_table} " \
                f"SELECT * FROM {self.CONFIG.SCHEMA}.{orig_table};"
        self.cur.execute(query)
        self.conn.commit()

    def backup_tables(self, table_list: list):
        """
        Создает резервные копии таблиц в БД. Возвращает True, если таблицы резервной копии созданы, иначе False.
        Аргументы:
            table_list:List[str] - список таблиц для резервной копии.
        """
        logging.info("Выполняю создание резервной копии таблиц OSM...")
        for table in table_list:
            try:
                logging.info(f"Создаю резервную копию таблицы {table}...")
                self._copy_table(table, f"{table}_backup")
            except Exception as err:
                logging.warning(f"Ошибка при создании резервной копии таблицы {table}: {err}")
                return False
        logging.info("Резервные копии таблиц созданы.")
        return True

    def restore_tables(self, table_list: list):
        """
        Восстанавливает резервные копии таблиц в БД. Возвращает True, если таблицы восстановлены, иначе False.
        Аргументы:
            table_list:List[str] - список таблиц для восстановления резервной копии.
        """
        logging.info("Выполняю восстановление резервной копии таблиц...")
        for table in table_list:
            try:
                logging.info(f"Восстанавливаю резервную копию таблицы {table}...")
                self._copy_table(f"{table}_backup", table)
            except Exception as err:
                logging.error(f"Ошибка при восстановлении резервной копии таблицы {table}: {err}. "
                              "Прекращаю восстановление.")
                return False
        logging.info("Таблицы восстановлены.")
        return True

    def drop_tables(self, table_list: list):
        """
        Удаляет таблицы из БД. Возвращает True, если таблицы удалены, иначе False.
        Аргументы:
            table_list:List[str] - список таблиц для удаления.
        """
        for table in table_list:
            try:
                query = f"DROP TABLE IF EXISTS {self.CONFIG.SCHEMA}.{table};"
                self.cur.execute(query)
                self.conn.commit()
            except Exception as err:
                logging.warning(f"Не удалось удалить таблицу {table}: {err}")
                return False
        return True

    def update_osm_tables(self):
        """
        Обновляет таблицы OSM в БД.
        """
        args = [
            'osm2pgsql',
            f'--database=postgresql://{self.CONFIG.USERNAME}:{self.CONFIG.PASSWORD}'
            f'@{self.CONFIG.HOST}:{self.CONFIG.PORT}/'
            f'{self.CONFIG.DATABASE_NAME}',
            '-c',
            os.path.join(self.DOWNLOADS, self.PBF_FILENAME),
        ]
        logging.info("Начинаю обновление БД...")
        process = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if process.returncode != 0:
            logging.fatal(f"Обновление БД завершилось неудачно. osm2pgsql выдал код ошибки {process.returncode}: \n"
                          f"{process.stderr.decode('utf-8')}")
            return False
        return True

    def create_layers(self):
        logging.info("Создаю слои...")
        backup_is_done = self.backup_layers()
        layers_created = True
        self.drop_tables(list(self.LAYERS.keys()))
        for layer, query in self.LAYERS.items():
            try:
                logging.info(f"Создаю слой {layer}...")
                self.cur.execute(query)
                self.conn.commit()
            except Exception as err:
                logging.error(f'Ошибка при создании слоя {layer}: {err}')
                layers_created = False
                break
        if not layers_created:
            logging.fatal("Не удалось создать слои. Обновление прервано.")
            sys.exit(1)
        if backup_is_done:
            self.drop_tables([f'{t}_backup' for t in self.LAYERS.keys()])
        logging.info("Слои созданы.")

    def backup_layers(self):
        logging.info("Создаю резервные копии слоев...")
        for layer in self.LAYERS.keys():
            try:
                logging.info(f"Создаю резервную копию слоя {layer}...")
                self._copy_table(layer, f"{layer}_backup")
            except Exception as err:
                logging.warning(f'Ошибка при создании резервной копии слоя {layer}: {err}')
                return False
        return True

    def restore_layers(self):
        logging.info("Восстанавливаю слои...")
        for layer in self.LAYERS.keys():
            try:
                logging.info(f"Восстанавливаю слой {layer}...")
                self._copy_table(f"{layer}_backup", layer)
            except Exception as err:
                logging.fatal(f'Ошибка при восстановлении резервной копии слоя {layer}: {err}. Обновление прервано.')
                sys.exit(1)

    def drop_layers(self, backup: bool = True):
        logging.info("Удаляю слои...")
        postfix = ""
        if backup:
            postfix = "_backup"
        for layer in self.LAYERS.keys():
            try:
                logging.info(f"Удаляю слой {layer}...")
                query = f"DROP TABLE IF EXISTS {self.CONFIG.SCHEMA}.{layer}{postfix};"
                self.cur.execute(query)
                self.conn.commit()
            except Exception as err:
                logging.error(f'Ошибка при удалении слоя {layer}: {err}')
                raise err


logging.basicConfig(level=logging.INFO)
app = OSMDownloader()
app.run(force_download=False, force_update=False, recreate_layers=True)

print(app.LAYERS['line_water'])
print(app.LAYERS['area_water'])
