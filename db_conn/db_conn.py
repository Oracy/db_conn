# Database
import datetime
import psycopg2
import json


def get_database_access(path=None):
    """
    Put a json file at your home directory:
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
    Parameters
    ----------
    :path: recieves access_information.json path
    """
    if path == None:
        path = '~/access_information.json'
        database_file_name = path
        with open(database_file_name, "r") as database_file:
            database_access = json.load(database_file)
        return database_access
    else:
        database_file_name = path
        with open(database_file_name, "r") as database_file:
            database_access = json.load(database_file)
        return database_access


class DatabaseHandler:
    """
    Handler for execute queries in a given the database
    """

    def __init__(self, access_information):
        """Class atributes

        Parameters
        ----------
        :access_information: databases access dictionary
        {
            "database_name": {
                "host": "database-db.host.net",
                "user": "user",
                "password": "1234",
                "database": "database_name",
                "port": 5432
            },
        }
        """
        self._host = access_information["host"]
        self._port = access_information.get("port", 5432)
        self._user = access_information["user"]
        self._password = access_information["password"]
        self._database = access_information["database"]
        self._connection = self._connect()

    @property
    def connection(self):
        """
        Connection attribute
        """
        return self._connection

    def _connect(self):
        """Establish connection with the database
        """
        connection_parameters = {
            "host": self._host,
            "port": self._port,
            "dbname": self._database,
            "user": self._user,
            "password": self._password
        }
        return psycopg2.connect(
            **connection_parameters, connect_timeout=10)

    def _reconnect(self):
        """Reconnect to database, if connection is closed
        """
        if self._connection.closed > 0:
            self._connection = self._connect()

    def close(self):
        """Close the connection
        """
        self._connection.close()

    def cursor(self):
        """Create cursors
        """
        return self.connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor)

    def db_connector(func):
        """
        Check the connection before making query,
        connect if disconnected

        Parameters
        ----------
        :func: Database related function which uses
        DatabaseHandler connection
        """
        def with_connection(self, *args, **kwargs):
            self._reconnect()
            try:
                result = func(self, *args, **kwargs)
            except Exception as error:
                print(f"Error: {error}")

            return result
        return with_connection

    @db_connector
    def fetch(self, query, params=None, max_tries=5):
        """
        Fetch query results

        Parameters
        ----------
        :query: Database related function which uses
        DatabaseHandler connection
        :params: Query params
        :max_tries: Max number of query retries

        Returns
        ----------
        Query results as a list of dicts
        """
        attempt_no = 0
        while attempt_no < max_tries:
            attempt_no += 1
            cursor = self.cursor()
            try:
                with self.connection:
                    with cursor:
                        cursor.execute(query, params)
                        return cursor.fetchall()
            except Exception as error:
                print(f"ERROR: In psycopg.cursor.fetchall(): {error}")
        return []

    @db_connector
    def query_to_df(self, sql, params=None, max_tries=5):
        """
        Create a pandas DataFrame object from a query result

        Parameters
        ----------
        :sql: query statements
        :params: a list or a tuple of parameters that will
        be passed to the query execution
        :max_tries: number of query retries in the case of failure

        Returns
        ----------
        Pandas DataFrame object
        """
        attempt_no = 0
        while attempt_no < max_tries:
            cursor = self.cursor()
            attempt_no += 1
            try:
                with self.connection:
                    with cursor:
                        return pd.read_sql_query(sql, self, params=params)
            except Exception as error:
                print(f"Query to DataFrame error: {error}.")

    @db_connector
    def get_first_id(self, function_schema, postgres_function,
                     schema, table_name, day_offset=0, hour_offset=1):
        """
        Get the id at the start of an interval

        Parameters
        ----------
        :function_schema:
        :postgres_function: can be "find_text_id" or "find_numeric_id"
        :schema: table schema
        :table_name: table_name
        :day_offset: day offset
        :hour_offset: hour offset

        Usage:
        get_first_id("public", "find_numeric_id", "public",
                     '"table name"', hour_offset=4)

        Returns
        ----------
        Dictionary {"first_id": id}
        """
        now = pytz.UTC.localize(datetime.datetime.utcnow())
        table_name = table_name
        function_schema = function_schema
        postgres_function = postgres_function
        schema = schema
        interval = datetime.timedelta(days=day_offset, hours=hour_offset)
        query = (
            f"select {function_schema}.{postgres_function}"
            f"('{schema}.{table_name}', %s) as first_id;"
        )
        result = None
        error_count = 0
        while result is None:
            error_count += 1
            if error_count >= 3:
                return None
            first_date = [(now-interval)]
            cur = self.cursor()
            with self.connection:
                with cur:
                    cur.execute(query, first_date)
                    result = cur.fetchall()
        return result[0]

    def query_between_dates_to_df(
            self, start, end, batch_size, sql, freq='D'):
        """
        Create a pandas DataFrame object from a queries results made
        in batches

        Parameters
        ----------
        :start: start date
        :end: end date
        :sql: query statements
        :freq: can be D(Days), Min(Minutes) or any other value specified
        in pandas "pd.date_range" docs
        :max_tries: number of query retries in the case of failure for each
        query executed

        Returns
        ----------
        Pandas DataFrame Object
        """
        df_dim_date = self.create_dim_date(start=start, end=end, freq='D')
        dim_date_list = df_dim_date.dimension_date.apply(str).tolist()

        frames = []
        batch_size = batch_size
        for index in range(0, len(dim_date_list), batch_size):
            date_batch = dim_date_list[index:index + batch_size]
            date_range = (date_batch[0], date_batch[-1])
            sql = sql
            try:
                df = self.query_to_df(sql, date_range, max_tries=1)
                frames.append(df)
                print('Executed')
                time.sleep(1)
            except Exception as error:
                print(f'Error: {error}')
            result = pd.concat(frames)
        return result

    @staticmethod
    def create_dim_date(start, end, tz='utc', freq='Min'):
        """
        Generates a date dimension table

        Parameters
        ----------
        :start: start date
        :end: end date
        :tz: time zone
        :freq: can be D(Days), Min(Minutes) or any other value specified
        in pandas "pd.date_range" docs

        Returns
        ----------
        Pandas DataFrame Object
        """
        df = pd.DataFrame(
            {"dimension_date": pd.date_range(start, end, freq=freq)})
        df["date_key"] = df.dimension_date.dt.strftime('%Y%m%d')
        df["dimension_timestamp_utc"] = (
            df.dimension_date.apply(
                lambda x: pd.Timestamp(x).tz_localize('utc')))
        df["dimension_timestamp_america_sp"] = (
            df.dimension_timestamp_utc.apply(
                lambda x: x.tz_convert('america/sao_paulo')))
        df["year"] = df.dimension_date.dt.year
        df["month"] = df.dimension_date.dt.month
        df["day"] = df.dimension_date.dt.day
        df["week"] = df.dimension_date.dt.weekofyear
        df["quarter"] = df.dimension_date.dt.quarter
        return df

    @staticmethod
    def load_query(path) -> str:
        """
        Load query from .sql file

        Parameters
        ----------
        :query: file.sql path

        Returns
        ----------
        String content of query file
        """
        with open(path, "r") as query_file:
            return query_file.read()
