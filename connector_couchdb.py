import couchdb
# import signal
# import time
from config import CONFIG #plik konfiguracyjny
import requests

"""
Tymczasowo porzucony - bug w bibliotece couchdb - błędy autoryzacji do połączenia z bazą
Nie zadziała - ciągłe błedy unauthorization error pomimo,tego , ze przed chwilą ten server user stworzył bazę o.Ó
https://github.com/cloudant/python-cloudant/issues/387

Rozwiązanie:
    'POST',
        self._session_url,
        data={'name': self._username, 'password': self._password},
        auth=(self._username, self._password)
    )
    resp.raise_for_status()

    
    inna biblioteka np. https://github.com/schferbe/python-cloudant #nie testowałam jeszcze
"""


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
    Wymagane konto server admina - stowrzyć je w futonie http://127.0.0.1:5984/_utils/#addAdmin/couchdb@localhost
    https://curl.trillworks.com/ Fajne narzędzie do zaminiania curl na składię request :)

    """
    def __init__(self, conf_dict): #slownik do konfiguracji bo duzo argumentow
        self.validate_args(conf_dict)
        self.__superuser = conf_dict["superuser"] # server admin
        self.__password = conf_dict["password"]
        self.__address = conf_dict["address"]
        self.__port = conf_dict["port"]
        self.__database = conf_dict["database"]
        self.check_connection_server()
        self.__db_server = self.get_connection()
        print(self.__db_server)

        is_created = self.create_database()
        if not is_created:
            self.cur = self.connect_database()

        # except ConnectorCouchdbException:
        #     raise ConnectorCouchdbException("Authorization error: can't create new database: {db_name}.".format(db_name = self.conf["database"]))

    def check_connection_server(self):
        try:
            response = requests.get('http://{superuser}:{password}@{address}:{port}/'.format(
                superuser = self.__superuser,
                password = self.__password,
                address = self.__address,
                port = self.__port
            ))
            print(response.status_code)
            if response.status_code != 200:
                raise ConnectorCouchdbException("Can't connect to couchdb - wrong credentials.")
        except requests.exceptions.ConnectionError as exception:
            raise ConnectorCouchdbException("Can't connect to server -couchdb engine isn't installed.")
        except BaseException as exception:
            raise ConnectionError("Can't use couchdb: {e}".format(e=exception))

    def validate_args(self, conf_dict):
        mandatory_keys = ["superuser", "password", "address", "port", "database"]
        missing = set(mandatory_keys ) - set(conf_dict.keys())
        if missing:
            raise ConnectorCouchdbException("Not enought elements to create object, missing: {missing}.".format(missing = list(missing)))

    def get_connection(self):
        """
        Zwraca connector do bazy danych
        domyślnie dla danych ze słownika, chyba że podano inaczej
        """
        try:
            return couchdb.Server("http://{superuser}:%{password}@{address}:{port}/".format(
                superuser = self.__superuser,
                password = self.__password,
                address = self.__address,
                port = self.__port,
                # db_name = self.__database
                ))

        except BaseException as exception:
            print("Znowu sie fąfla o.Ó")
            raise ConnectorCouchdbException("Couchdb nie można połączyć się z serwerem")

    def query(self, query):
        """
        Wykonywanie żadań na bazie
        """
        # self.cur
        pass

    def connect_database(self,):
        try:
            return self.__db_server[self.__database]
        except BaseException as exception:
            print("NO NIE :<")
        # else:
        # raise ConnectorCouchdbException("[ConnectorCouchdb] Can't connect to database - database: {db_name} doesn't exists".format( db_name = db_name))

    def create_database(self):
        """
        Łączy się z bazą z konfiguracji albo tworzy nową bazę
        Jeśli dane do logowania są nieprawidłowo to w nieskończonośc próbuje połączyć z serwerem

        to działa:
        -------------
            curl -X PUT http://superuser:password@127.0.0.1:5984/somedatabase2

        [!] to NIE działa [!]
        ---------------------
            connector = self.db_server.create(db_name)
            return connector[str(db_name)]

        """
        print("Create database")

        response = requests.put('http://{superuser}:{password}@{address}:{port}/{db_name}'.format(
            superuser = self.__superuser,
            password = self.__password,
            address = self.__address,
            port = self.__port,
            db_name = self.__database
        ))

        if response.status_code == 201:
            print("Created: {db_name}.".format(db_name =  self.__database))
            return True
        if response.status_code == 412:
            print("Database: {db_name} already exists.".format(db_name = self.__database))
            return False
        if response.status_code == 401:
            print("Can't created database: {db_name} - unauthorized error for user: {user}.".format(db_name=  self.__database, user = self.__superuser))
            raise ConnectorCouchdbException("Can't created database: {db_name} - unauthorized error for user: {user}.".format(db_name=  self.__database, user = self.__superuser))

    def delete_database(self, ):
        """
        Usuwanie bazy danych
        """
        if self.__database in self.__db_server:
            del self.__db_server[self.__database]
        else:
            raise ConnectorCouchdbException("[CouchDB] Can't remove database: {db_name} - db doesn't exists".format(db_name=self.__database))
    
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
            "superuser" : "superuser" ,
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