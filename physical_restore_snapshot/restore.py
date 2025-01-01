import shutil
import subprocess
import peewee
import os
import contextlib
import re

"""
Naming -
backup > The server, which holds the database
target > The server, where we want to restore the database

Directory -
/var/lib/mysql > Directory of Backup Server
/target_db > Directory of Target Server's Specific Database Directory

backup_db > Database Name of Backup Server
target_db > Database Name of Target Server

backup_db_root_password > Root Password of Backup Server
target_db_root_password > Root Password of Target Server

target_db_port > Port of Target Server
target_db_host > localhost

#TODO: Handle MyISAM tables
#TODO: log the supressed errors
"""


class ConnectionClosedWithDatabase(Exception):
    pass


class DatabaseSchemaExportError(Exception):
    pass


class DatabaseImporter:
    def __init__(
        self,
        target_db: str,
        backup_db: str,
        backup_db_root_password: str,
        target_db_root_password: str,
        target_db_port: int,
        target_db_host: str,
    ):
        self._target_db_instance: peewee.MySQLDatabase = None
        self._backup_db_instance: peewee.MySQLDatabase = None

        self.target_db = target_db
        self.target_db_user = "root"
        self.target_db_password = target_db_root_password
        self.target_db_host = target_db_host
        self.target_db_port = target_db_port
        self.target_db_directory = os.path.join("/target_db")

        self.backup_db = backup_db
        self.backup_db_user = "root"
        self.backup_db_password = backup_db_root_password
        self.backup_db_host = "localhost"
        self.backup_db_port = 3306
        self.backup_db_directory = os.path.join("/var/lib/mysql", self.backup_db)

        self.db_schema: str = ""
        self.tables: list[str] = []

    def process(self):
        self.get_backup_db()
        print("Validated connection with backup db")
        self.get_target_db()
        print("Validated connection with target db")
        print("-- Starting the process --")
        self._prepare_backup_db()
        print("Prepared backup db")
        # https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#importing-transportable-tablespaces-for-non-partitioned-tables
        self._prepare_target_db_for_restore()
        print("Prepared target db")
        self._create_tables_from_backup()
        print("Created tables from backup")
        self._discard_tablespaces_from_target_db()
        print("Discarded tablespaces from target db")
        self._perform_file_operations()
        print("Performed file operations")
        self._import_tablespaces_in_target_db()
        print("Imported tablespaces in target db")

    def _prepare_backup_db(self):
        # fetch table names
        self.tables = self.get_backup_db().get_tables()

        # flush tables with read lock
        flush_table_export_query = "FLUSH TABLES {} FOR EXPORT;".format(
            ", ".join(self.tables)
        )
        self.get_backup_db().execute_sql(flush_table_export_query)

        # export the database schema
        command = [
            "mariadb-dump",
            "-u",
            self.backup_db_user,
            "-p" + self.backup_db_password,
            "--no-data",
            self.backup_db,
        ]
        try:
            output = subprocess.check_output(command)
        except subprocess.CalledProcessError as e:
            raise DatabaseSchemaExportError(e.output)
        self.db_schema = output.decode("utf-8")

    def _prepare_target_db_for_restore(self):
        """
        Prepare the database for import
        - fetch existing tables list in database
        - delete all tables
        - take look at db_directory whether any *.ibd files are present [Raise exception if any]
        """
        tables = self.get_target_db().get_tables()
        # before start dropping tables, disable foreign key checks
        # it will reduce the time to drop tables and will not cause any block while dropping tables
        self.get_target_db().execute_sql("SET SESSION FOREIGN_KEY_CHECKS = 0;")
        for table in tables:
            with contextlib.suppress(Exception):
                self.get_target_db().execute_sql("DROP TABLE {};".format(table))
        self.get_target_db().execute_sql(
            "SET SESSION FOREIGN_KEY_CHECKS = 1;"
        )  # re-enable foreign key checks

        # check if there are any *.ibd files in the db_directory
        files_in_db_directory = os.listdir(self.target_db_directory)
        if any([file.endswith(".ibd") for file in files_in_db_directory]):
            raise Exception("Database directory still contains *.ibd files.")

    def _create_tables_from_backup(self):
        # extract only the schema.sql file from tar
        # https://github.com/frappe/frappe/pull/26855
        schema_file_content = re.sub(
            r"/\*M{0,1}!999999\\- enable the sandbox mode \*/",
            "",
            self.db_schema,
        )
        # # https://github.com/frappe/frappe/pull/28879
        schema_file_content = re.sub(
            r"/\*![0-9]* DEFINER=[^ ]* SQL SECURITY DEFINER \*/",
            "",
            self.db_schema,
        )
        # create the tables
        sql_stmts = schema_file_content.split(";\n")
        for sql_stmt in sql_stmts:
            if sql_stmt.strip():
                self.get_target_db().execute_sql(sql_stmt)

    def _discard_tablespaces_from_target_db(self):
        # https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#foreign-key-constraints
        self.get_target_db().execute_sql("SET SESSION foreign_key_checks = 0;")
        for table in self.tables:
            self.get_target_db().execute_sql(
                "ALTER TABLE {} DISCARD TABLESPACE;".format(table)
            )
        self.get_target_db().execute_sql(
            "SET SESSION foreign_key_checks = 1;"
        )  # re-enable foreign key checks

    def _perform_file_operations(self):
        for table in self.tables:
            table_ibd_file = table + ".ibd"
            table_ibd_file_path = os.path.join(self.backup_db_directory, table_ibd_file)
            if os.path.exists(table_ibd_file_path):
                shutil.copy(
                    os.path.join(self.backup_db_directory, table_ibd_file),
                    os.path.join(self.target_db_directory, table_ibd_file),
                )
            table_cfg_file = table + ".cfg"
            shutil.copy(
                os.path.join(self.backup_db_directory, table_cfg_file),
                os.path.join(self.target_db_directory, table_cfg_file),
            )

        # change ownership to mysql user and group and mode to 660
        for file in os.listdir(self.target_db_directory):
            file_path = os.path.join(self.target_db_directory, file)
            os.chmod(file_path, 0o660)
            shutil.chown(file_path, user="mysql", group="mysql")

    def _import_tablespaces_in_target_db(self):
        for table in self.tables:
            self.get_target_db().execute_sql(
                "ALTER TABLE {} IMPORT TABLESPACE;".format(table)
            )

    # private methods
    def get_backup_db(self) -> peewee.MySQLDatabase:
        if self._backup_db_instance is not None:
            if not self._backup_db_instance.is_connection_usable():
                raise ConnectionClosedWithDatabase()
            return self._backup_db_instance

        print("Creating backup db instance")
        self._backup_db_instance = peewee.MySQLDatabase(
            self.backup_db,
            user=self.backup_db_user,
            password=self.backup_db_password,
            host=self.backup_db_host,
            port=self.backup_db_port,
        )
        self._backup_db_instance.connect()
        return self._backup_db_instance

    def get_target_db(self) -> peewee.MySQLDatabase:
        if self._target_db_instance is not None:
            if not self._target_db_instance.is_connection_usable():
                raise ConnectionClosedWithDatabase()
            return self._target_db_instance

        self._target_db_instance = peewee.MySQLDatabase(
            self.target_db,
            user=self.target_db_user,
            password=self.target_db_password,
            host=self.target_db_host,
            port=self.target_db_port,
        )
        self._target_db_instance.connect()
        # Set session wait timeout to 4 hours [EXPERIMENTAL]
        self._target_db_instance.execute_sql("SET SESSION wait_timeout = 14400;")
        return self._target_db_instance

    def __del__(self):
        if self._target_db_instance is not None:
            self._target_db_instance.close()
        if self._backup_db_instance is not None:
            self._backup_db_instance.close()


if __name__ == "__main__":
    importer = DatabaseImporter(
        target_db=os.environ.get("TARGET_DB"),
        backup_db=os.environ.get("BACKUP_DB"),
        backup_db_root_password=os.environ.get("BACKUP_DB_ROOT_PASSWORD"),
        target_db_root_password=os.environ.get("TARGET_DB_ROOT_PASSWORD"),
        target_db_port=int(os.environ.get("TARGET_DB_PORT", 3306)),
        target_db_host=os.environ.get("TARGET_DB_HOST", "host.docker.internal"),
    )
    importer.process()
