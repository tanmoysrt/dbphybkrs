import peewee
import os
import contextlib
import tarfile
import re

class DatabaseImporter:
    def __init__(self, database:str, db_user:str, db_password:str, target_restore_file_path:str, db_host:str="localhost", db_port:int=3306, db_base_path:str="/var/lib/mysql"):
        self._db_instance:peewee.MySQLDatabase = None
        self._db_tables_locked:bool = False

        self.database = database
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_base_path = db_base_path
        self.db_directory = os.path.join(self.db_base_path, self.database)
        self.target_restore_file_path = target_restore_file_path
        if not target_restore_file_path.endswith(".tar.gz") and not target_restore_file_path.endswith(".tar"):
            raise ValueError("Target restore path should end with .tar.gz or .tar")

        self.tables:list[str] = []

    def process(self):
        # https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#importing-transportable-tablespaces-for-non-partitioned-tables
        self._prepare_database_for_restore()
        self._create_tables_from_backup()
        self.tables = self.get_db().get_tables()
        self._discard_tablespaces()
        self._perform_file_operations()
        self._import_tablespaces()

    def _prepare_database_for_restore(self):
        """
        Prepare the database for import
        - fetch existing tables list in database
        - delete all tables
        - take look at db_directory whether any *.ibd files are present [Raise exception if any]
        """
        tables = self.get_db().get_tables()
        # before start dropping tables, disable foreign key checks
        # it will reduce the time to drop tables and will not cause any block while dropping tables
        self.get_db().execute_sql("SET SESSION FOREIGN_KEY_CHECKS = 0;")
        for table in tables:
            with contextlib.suppress(Exception) as e:
                self.get_db().execute_sql("DROP TABLE {};".format(table))
        self.get_db().execute_sql("SET SESSION FOREIGN_KEY_CHECKS = 1;") # re-enable foreign key checks

        # check if there are any *.ibd files in the db_directory
        files_in_db_directory = os.listdir(self.db_directory)
        if any([file.endswith(".ibd") for file in files_in_db_directory]):
            raise Exception("Database directory still contains *.ibd files.")
        
    def _create_tables_from_backup(self):
        # extract only the schema.sql file from tar
        with tarfile.open(self.target_restore_file_path, "r:gz") as tar_file:
            schema_file = tar_file.extractfile("schema.sql")
            schema_file_content = schema_file.read().decode("utf-8")
            # https://github.com/frappe/frappe/pull/26855
            schema_file_content = re.sub(r"/\*M{0,1}!999999\\- enable the sandbox mode \*/", "", schema_file_content)
            # # https://github.com/frappe/frappe/pull/28879
            schema_file_content = re.sub(r"/\*![0-9]* DEFINER=[^ ]* SQL SECURITY DEFINER \*/", "", schema_file_content)
            # create the tables
            sql_stmts = schema_file_content.split(";")
            for sql_stmt in sql_stmts:
                if sql_stmt.strip():
                    self.get_db().execute_sql(sql_stmt)
    
    def _discard_tablespaces(self):
        # https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#foreign-key-constraints
        self.get_db().execute_sql("SET SESSION foreign_key_checks = 0;")
        for table in self.tables:
            self.get_db().execute_sql("ALTER TABLE {} DISCARD TABLESPACE;".format(table))
        self.get_db().execute_sql("SET SESSION foreign_key_checks = 1;") # re-enable foreign key checks

    def _perform_file_operations(self):
        with tarfile.open(self.target_restore_file_path, "r:gz") as tar_file:
            for table in self.tables:
                table_ibd_file = table + ".ibd"
                tar_file.extract(table_ibd_file, self.db_directory)
                table_cfg_file = table + ".cfg"
                tar_file.extract(table_cfg_file, self.db_directory)
            # TODO: change the ownership to mysql user

    def _import_tablespaces(self):
        for table in self.tables:
            self.get_db().execute_sql("ALTER TABLE {} IMPORT TABLESPACE;".format(table))

    # private methods
    def get_db(self) -> peewee.MySQLDatabase:
        if self._db_instance is not None and self._db_instance.is_connection_usable():
            return self._db_instance
        self._db_instance = peewee.MySQLDatabase(self.database, user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port)
        self._db_instance.connect()
        # Set session wait timeout to 4 hours [EXPERIMENTAL]
        self._db_instance.execute_sql("SET SESSION wait_timeout = 14400;") 
        return self._db_instance

    def __del__(self):
        if self._db_instance is not None:
            self._db_instance.close()
