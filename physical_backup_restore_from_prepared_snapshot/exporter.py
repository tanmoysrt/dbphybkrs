import peewee
import os
import subprocess


class DatabaseSchemaExportError(Exception):
    pass


class DatabaseExportFileNotFoundError(Exception):
    pass


class DatabaseConnectionClosedWithDatabase(Exception):
    pass


class DatabaseExporter:
    def __init__(
        self,
        databases: list[str],
        db_user: str,
        db_password: str,
        db_host: str = "localhost",
        db_port: int = 3306,
        db_base_path: str = "/var/lib/mysql",
    ):
        # Instance variable for internal use
        self._db_instances: dict[str, peewee.MySQLDatabase] = {}
        self._db_tables_locked: dict[str, bool] = {db: False for db in databases}

        # variables
        self.databases = databases
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_base_path = db_base_path
        self.db_directories: dict[str, str] = {
            db: os.path.join(self.db_base_path, db) for db in self.databases
        }

        self.innodb_tables: dict[str, list[str]] = {db: [] for db in self.databases}
        self.myisam_tables: dict[str, list[str]] = {db: [] for db in self.databases}
        self.table_schemas: dict[str, str] = {}

    def process(self):
        self._gather_required_info()
        self._prepare_databases_for_disk_snapshot()
        self._validate_exportable_files()

        data = {}
        for db_name in self.databases:
            data[db_name] = {
                "innodb_tables": self.innodb_tables[db_name],
                "myisam_tables": self.myisam_tables[db_name],
                "table_schemas": self.table_schemas[db_name],
            }

        """
        Dump it to a file for now.

        Need to be sent to press. Important to restore the database later.
        """
        with open("snapshot.json", "w") as f:
            f.write(str(data))

        print("After taking snapshot press enter to continue")
        input()  # Wait for user to take snapshot
        self.unlock_all_tables()

    def _gather_required_info(self):
        """
        Store the table names and their engines in the respective dictionaries
        """
        for db_name in self.databases:
            db_instance = self.get_db(db_name)
            query = (
                "SELECT table_name, ENGINE FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_type != 'VIEW' "
                "ORDER BY table_name"
            )
            data = db_instance.execute_sql(query).fetchall()
            for row in data:
                table = row[0]
                engine = row[1]
                if engine == "InnoDB":
                    self.innodb_tables[db_name].append(table)
                elif engine == "MyISAM":
                    self.myisam_tables[db_name].append(table)

    def _prepare_databases_for_disk_snapshot(self):
        for db_name in self.databases:
            """
            InnoDB and MyISAM tables
            Flush the tables and take read lock

            Ref : https://mariadb.com/kb/en/flush-tables-for-export/#:~:text=If%20FLUSH%20TABLES%20...%20FOR%20EXPORT%20is%20in%20effect%20in%20the%20session%2C%20the%20following%20statements%20will%20produce%20an%20error%20if%20attempted%3A

            FLUSH TABLES ... FOR EXPORT
            This will
                - Take READ lock on the tables
                - Flush the tables
                - Will not allow to change table structure (ALTER TABLE, DROP TABLE nothing will work)
            """
            tables = self.innodb_tables[db_name] + self.myisam_tables[db_name]
            flush_table_export_query = "FLUSH TABLES {} FOR EXPORT;".format(
                ", ".join(tables)
            )
            self.get_db(db_name).execute_sql(flush_table_export_query)
            self._db_tables_locked[db_name] = True

            """
            Export the database schema
            It's important to export the schema only after taking the read lock.
            """
            self.table_schemas[db_name] = self.export_table_schema(db_name)

    def _validate_exportable_files(self):
        for db_name in self.databases:
            # list all the files in the database directory
            db_files = os.listdir(self.db_directories[db_name])
            """
            InnoDB tables should have the .cfg files to be able to restore it back

            https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#exporting-transportable-tablespaces-for-non-partitioned-tables
            """
            for table in self.innodb_tables[db_name]:
                table_file = table + ".ibd"
                if table_file not in db_files:
                    raise DatabaseExportFileNotFoundError(
                        "IBD file for table {} not found".format(table)
                    )
            """
            MyISAM tables should have .MYD and .MYI files at-least to be able to restore it back
            """
            for table in self.myisam_tables[db_name]:
                table_files = [table + ".MYD", table + ".MYI"]
                for table_file in table_files:
                    if table_file not in db_files:
                        raise DatabaseExportFileNotFoundError(
                            "MYD or MYI file for table {} not found".format(table)
                        )

    def export_table_schema(self, db_name) -> str:
        command = [
            "mariadb-dump",
            "-u",
            self.db_user,
            "-p" + self.db_password,
            "--no-data",
            db_name,
        ]
        try:
            output = subprocess.check_output(command)
        except subprocess.CalledProcessError as e:
            raise DatabaseSchemaExportError(e.output)

        return output.decode("utf-8")

    def unlock_all_tables(self):
        for db_name in self.databases:
            self._unlock_tables(db_name)

    def _unlock_tables(self, db_name):
        self.get_db(db_name).execute_sql("UNLOCK TABLES;")
        self._db_tables_locked[db_name] = False
        """
        Anyway, if the db connection gets closed or db thread dies,
        the tables will be unlocked automatically
        """

    def get_db(self, db_name: str) -> peewee.MySQLDatabase:
        instance = self._db_instances.get(db_name, None)
        if instance is not None:
            if not instance.is_connection_usable():
                raise DatabaseConnectionClosedWithDatabase(
                    "Database connection closed with database {}".format(db_name)
                )
            return instance
        if db_name not in self.databases:
            raise ValueError("Database {} not found".format(db_name))
        self._db_instances[db_name] = peewee.MySQLDatabase(
            db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
        )
        self._db_instances[db_name].connect()
        # Set session wait timeout to 4 hours [EXPERIMENTAL]
        self._db_instances[db_name].execute_sql("SET SESSION wait_timeout = 14400;")
        return self._db_instances[db_name]

    def __del__(self):
        for db_name in self.databases:
            if self._db_tables_locked[db_name]:
                self._unlock_tables(db_name)

        for db_name in self.databases:
            self.get_db(db_name).close()


if __name__ == "__main__":
    exporter = DatabaseExporter(
        databases=["askubuntu"],
        db_user="root",
        db_password="toor",
    )
    exporter.process()
