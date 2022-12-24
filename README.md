# kvdb
Implementation of kv store, not for production.

Example:
```
from kvdb import Kvdb
db = Kvdb("exampledb")
db.put("message", "Hello")
print(db.get("message"))
```
