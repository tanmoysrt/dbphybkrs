### Usage

**Export**

```
from .exporter import DatabaseExporter

if __name__ == "__main__":
    b = DatabaseExporter("employees", "root", "toor", "/root/phybkrs/dumpp/2.tar.gz")
    b.process()
```

**Import**
```
from .importer import DatabaseImporter

if __name__ == "__main__":
    b = DatabaseImporter("test3", "root", "toor", "/root/phybkrs/dumpp/2.tar.gz")
    b.process()
```