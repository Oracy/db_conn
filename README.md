# Python Package

## db_conn

To use this package is necessary create file on your home `~`

```bash
    "~/access_information.json"
        {
            "database_name": {
                "host": "database-db.host.net",
                "user": "user",
                "password": "1234",
                "database": "database_name",
                "port": 5432
            },
        }
```

<div class="panel panel-gitlab-orange">
    **Warning**
    {: .panel-heading}
    <div class="panel-body">
        Remember, if you put your `access_information` inside of your project, add this file into `.gitignore` even if it is a private project, security first 😉
    </div>
</div>


After that to use in code, you just need to:

```python
# if you do not use path var, it will assume default path as ~/access_information.json, if you want to use another path you can pass this new path

# You should pass the 'database_name' that you put on your access_information

# With path:
path = os.path.expanduser("~/access_information.json")
database_access = get_database_access(path)
db_handler = DatabaseHandler(database_access["database_name"])

###########################################################################

# You should pass the 'database_name' that you put on your access_information

# Without path
database_access = get_database_access()
db_handler = DatabaseHandler(database_access["database_name"])
```

To use this connection properly

```python
query = """
    select 
      *
    from table;
"""
df = pd.DataFrame(db_handler.fetch(query))
```

After use, you should close connection

```python
db_handler.close()
```

---

To create table with time series you can use code below, passing start_date, end_date, timezone and frequency

```python
df_script_dim_table = db_handler.create_dim_date('2020-01-01', '2020-05-06', tz='utc', freq='D')
```
