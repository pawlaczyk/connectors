import couchdb
# import signal
# import time
from config import CONFIG #plik konfiguracyjny
import requests

class ConnectorCouchdbException(Exception):
    """Raise for my specific kind of exception from ConnectorClass"""

def timeout_handler(num, stack):
    print("Received SIGALRM")
    raise ConnectorCouchdbException("Can't connect to server.")

class ConnectorCouchdb():
    """
    Konektor do bazy dla loggera
    405 - method not allowed
    412 - database already exists

    Note:
    -----
    curl -X GET http://localhost:5984/_membership --user admin 

    """
    def __init__(self, conf_dict): #slownik do konfiguracji bo duzo argumentow
        self.conf = conf_dict
        self.validate_args()
        self.check_connection_server()
        self.db_server = self.get_connection()
        # # try:
        self.cur = self.create_database()
        # except couchdb.http.Unauthorized as exception:
        #     self.cur = self.connect_database()
        # except ConnectorCouchdbException:
        #     raise ConnectorCouchdbException("Authorization error: can't create new database: {db_name}.".format(db_name = self.conf["database"]))

    def check_connection_server(self):
        user = self.conf["user"]
        password = self.conf["password"]
        address = self.conf["address"]
        port =self.conf["port"]
        try:
            response = requests.get('http://{user}:{password}@{address}:{port}/'.format(
                user = user,
                password = password,
                address = address,
                port = port
            ),
            verify=False)
            print(response.status_code)
            if response.status_code != 200:
                raise ConnectorCouchdbException("Can't connect to couchdb - wrong credentials.")
        except requests.exceptions.ConnectionError as exception:
            raise ConnectorCouchdbException("Can't connect to server -couchdb engine isn't installed.")
        except BaseException as exception:
            raise ConnectionError("Can't use couchdb: {e}".format(e=exception))

    def validate_args(self, conf_dict=None):
        conf_dict = conf_dict if conf_dict else self.conf

        mandatory_keys = ["user", "password", "address", "port", "database"]
        missing = set(mandatory_keys ) - set(conf_dict.keys())
        if missing:
            raise ConnectorCouchdbException("Not enought elements to create object, missing: {missing}.".format(missing = list(missing)))

    def get_connection(self, user=None, password=None, address= None,  port=None):
        """
        Zwraca connector do bazy danych
        domyślnie dla danych ze słownika, chyba że podano inaczej
        """
        user = user if user else self.conf["user"]
        password = password if password else self.conf["password"]
        address = address if address else self.conf["address"]
        port = port if port else self.conf["port"]
        print("user: ", user)

        try:
            return couchdb.Server("http://{user}:%{password}@{address}:{port}/".format(
                user = user,
                password = password,
                address = address,
                port = port
                ))

        except BaseException as exception:
            raise ConnectorCouchdbException("Couchdb nie można połączyć się z serwerem")

    def query(self, query):
        """
        Wykonywanie żadań na bazie
        """
        # self.cur
        pass

    def connect_database(self, db_name=None) :
        print("db_name: ", db_name)
        db_name = db_name if db_name else self.conf["database"]
        print("Connect database")
        if db_name in self.__db_server:
            return self.__db_server[db_name]
        else:
            raise ConnectorCouchdbException("[ConnectorCouchdb] Can't connect to database - database: {db_name} doesn't exists".format( db_name = db_name))

    def create_database(self, db_name=None):
        """
        Łączy się z bazą z konfiguracji albo tworzy nową bazę
        Jeśli dane do logowania są nieprawidłowo to w nieskończonośc próbuje połączyć z serwerem
        """
        print("Create database")

        db_name = db_name if db_name else self.conf["database"]
        print("ZMIENNA: ", db_name)
        # if db_name in self.__db_server:
        #     raise ConnectorCouchdbException("Can't create database : {db_name} already exists".format(db_name= db_name))
        # else:
        # try:
        connector = self.db_server.create(db_name)
        return connector[str(db_name)]
        # except couchdb.http.Unauthorized:
        self.db_server["go_test"]
            # raise ConnectorCouchdbException("Can't creae database {db_name} - authorization refused.")



    def delete_database(self, db_name=None):
        """
        Usuwanie bazy danych
        """
        db_name = db_name if db_name else self.conf["database"]
        if db_name in self.__db_server:
            del self.__db_server[db_name]
        else:
            raise ConnectorCouchdbException("[CouchDB] Can't remove database: {db_name} - db doesn't exists".format(db_name=db_name))
    
    def __del__(self):
        """
        Zamyka połaczenie z bazą i usuwa obiekt
        """
        if hasattr(self, 'cur'):
            if self.cur:
                del self.cur



if __name__ == "__main__":
    conf_test = {
        "couchdb": {
            "address" : "127.0.0.1",
            "port" : "5984",
            "user" : "admin" ,
            "password": "password",
            "database": "my_database"
        }
    }

    try:
        c_obj = ConnectorCouchdb(conf_test["couchdb"])
    except BaseException as exception:
        print(exception)
        print(type(exception))
    # try:
    #     a = couchdb.Server("https://admin:%password@127.0.0.1:5984/")
    #     print(a)

    # except BaseException as exception:
    #     print(exception)