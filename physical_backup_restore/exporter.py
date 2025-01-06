import io
import peewee
import os
import subprocess
import tarfile


class DatabaseSchemaExportError(Exception):
    pass


class DatabsaeExportFileNotFoundError(Exception):
    pass


class DatabaseExporter:
    def __init__(
        self,
        database: str,
        db_user: str,
        db_password: str,
        target_backup_file_path: str,
        db_host: str = "localhost",
        db_port: int = 3306,
        db_base_path: str = "/var/lib/mysql",
    ):
        # Instance variable for internal use
        self._db_instance: peewee.MySQLDatabase = None
        self._db_tables_locked: bool = False

        # Other variables
        self.database = database
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_base_path = db_base_path
        self.db_directory = os.path.join(self.db_base_path, self.database)
        self.target_backup_file_path = target_backup_file_path
        if not target_backup_file_path.endswith(".tar.gz"):
            raise ValueError("Target backup path should end with .tar.gz")

        self.tables: list[str] = []
        self.table_schema: str = ""

    def process(self):
        self._perform_database_operations()
        self._perform_file_operations()
        self._unlock_tables()

    def _perform_database_operations(self):
        # get the tables
        self.tables = self.get_db().get_tables()
        # flush on the fly data to disk and take read lock on tables
        flush_table_export_query = "FLUSH TABLES {} FOR EXPORT;".format(
            ", ".join(self.tables)
        )
        self.get_db().execute_sql(flush_table_export_query)
        self._db_tables_locked = True
        # export the database schema
        self.table_schema = self.export_table_schema()

    def _perform_file_operations(self):
        # list all the files in the database directory
        db_files = os.listdir(self.db_directory)
        """
        validate db files
        every table should have .ibd and .cfg file
        We need only those two files to export

        https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#exporting-transportable-tablespaces-for-non-partitioned-tables
        """
        for table in self.tables:
            table_file = table + ".ibd"
            if table_file not in db_files:
                raise DatabsaeExportFileNotFoundError(
                    "IBD file for table {} not found".format(table)
                )
            table_cfg_file = table + ".cfg"
            if table_cfg_file not in db_files:
                raise DatabsaeExportFileNotFoundError(
                    "CFG file for table {} not found".format(table)
                )

        # if backup file already exists, delete it
        if os.path.exists(self.target_backup_file_path):
            os.remove(self.target_backup_file_path)

        # create a tar file (gzipped)
        with tarfile.open(
            self.target_backup_file_path, "w:gz", compresslevel=1
        ) as tar_file:
            # add the schema file
            schema_bytes = self.table_schema.encode("utf-8")
            schema_info = tarfile.TarInfo(name="schema.sql")
            schema_info.size = len(schema_bytes)
            tar_file.addfile(schema_info, io.BytesIO(schema_bytes))

            # add the tables's ibd and cfg files
            for table in self.tables:
                table_ibd_file = table + ".ibd"
                tar_file.add(
                    os.path.join(self.db_directory, table_ibd_file),
                    arcname=table_ibd_file,
                )
                table_cfg_file = table + ".cfg"
                tar_file.add(
                    os.path.join(self.db_directory, table_cfg_file),
                    arcname=table_cfg_file,
                )

    def export_table_schema(self) -> str:
        command = [
            "mariadb-dump",
            "-u",
            self.db_user,
            "-p" + self.db_password,
            "--no-data",
            self.database,
        ]
        try:
            output = subprocess.check_output(command)
        except subprocess.CalledProcessError as e:
            raise DatabaseSchemaExportError(e.output)

        return output.decode("utf-8")

    def _unlock_tables(self):
        self.get_db().execute_sql("UNLOCK TABLES;")
        self._db_tables_locked = False
        """
        Anyway, if the db connection gets closed or db thread dies,
        the tables will be unlocked automatically
        """

    def get_db(self) -> peewee.MySQLDatabase:
        if self._db_instance is not None and self._db_instance.is_connection_usable():
            return self._db_instance
        self._db_instance = peewee.MySQLDatabase(
            self.database,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
        )
        self._db_instance.connect()
        # Set session wait timeout to 4 hours [EXPERIMENTAL]
        self._db_instance.execute_sql("SET SESSION wait_timeout = 14400;")
        return self._db_instance

    def __del__(self):
        if self._db_tables_locked:
            # in case of an exception, unlock the tables
            self._unlock_tables()

        if self._db_instance is not None:
            self._db_instance.close()
