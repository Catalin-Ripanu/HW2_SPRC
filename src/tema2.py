from flask import Flask, jsonify, Response, request, json
from flask import Blueprint
from datetime import datetime
import time
import MySQLdb
from os import getenv

# IMPORTANT
# Se poate executa toata arhitectura folosind comanda 'make build'

server = None
config = None

t0 = time.localtime().tm_sec / 10**3
t1 = time.localtime().tm_sec / 10**3

#   Functie generica pentru procesarea cererilor
#   ce sunt folosite in toate cele 3 tabele date (Orase, Tari, Temperaturi)


def generic_process_post_get_pair(handler):
    # In POST nu este nevoie de id

    if ("id", [int]) in handler.fields_list:
        handler.fields_list.remove(("id", [int]))

    if handler.request.method == "POST":
        # Se verifica validitatea cererii
        is_wrong = False
        for body_info in handler.fields_list:
            if (
                body_info[0] not in handler.request.json
                or type(request.json[body_info[0]]) not in body_info[1]
            ):
                is_wrong = True
                break
        if is_wrong == True:
            return Response(status=400)

        # Se construieste cererea cu informatiie corecte din .json
        aux_lst = []
        extract_fields = []
        for elem in handler.fields_list:
            extract_fields.append(elem[0])
            aux = handler.request.json[elem[0]]
            if str == type(aux):
                var = "'{}'".format(aux)
                aux_lst.append("{}".format(var))
            else:
                aux_lst.append("{}".format(aux))

        body_ins = "(" + ", ".join(aux_lst) + ")"
        cols_ins = "(" + ", ".join(extract_fields) + ")"

        # Se trimite cererea sub forma unui query SQL
        try:
            server.cursor.execute(
                f"INSERT INTO {handler.database} {cols_ins} VALUES {body_ins}"
            )
            server.connection.commit()
        except:
            return Response(status=409)

        # Se intoarce ultimul id introdus in tabela
        server.cursor.execute("SELECT {}".format("LAST_INSERT_ID()"))
        return Response(
            status=201,
            response=json.dumps({"id": server.cursor.fetchone()[0]}),
            mimetype="application/json",
        )
    else:
        # In GET este nevoie de id
        if handler.fields_list[0][0] != "id":
            handler.fields_list.insert(0, ("id", [int]))

        # Se trimite query-ul SQL catre tabela respectiva
        server.cursor.execute(f"SELECT * FROM {handler.database}")

        # Se construieste cererea
        body_info = []
        for elem in server.cursor.fetchall():
            aux_dic = {}
            for iter in range(len(handler.fields_list)):
                aux_dic.update({handler.fields_list[iter][0]: elem[iter]})
            body_info.append(aux_dic)
        return Response(
            status=200, response=json.dumps(body_info), mimetype="application/json"
        )


#   Functie generica pentru procesarea cererilor
#   ce sunt folosite in toate cele 3 tabele date (Orase, Tari, Temperaturi)


def generic_process_put_del_pair(handler, id):
    # Se verifica daca id-ul este valid
    try:
        result = int(id)
    except ValueError:
        return Response(status=404)

    if handler.request.method == "PUT":
        # Se verifica validitatea cererii
        is_wrong = False
        for body_info in handler.fields_list:
            if (
                body_info[0] not in handler.request.json
                or type(request.json[body_info[0]]) not in body_info[1]
            ):
                is_wrong = True
                break
        if is_wrong == True or handler.request.json["id"] != result:
            return Response(status=400)

        # In PUT nu este nevoie de id
        if ("id", [int]) in handler.fields_list:
            handler.fields_list.remove(("id", [int]))

        # Se construieste interogarea SQL
        aux_lst = []
        for elem in handler.fields_list:
            aux = handler.request.json[elem[0]]
            if str == type(aux):
                var = "'{}'".format(aux)
                aux_lst.append("{} = {}".format(elem[0], var))
            else:
                aux_lst.append("{} = {}".format(elem[0], aux))

        try:
            new_body = ", ".join(aux_lst)
            server.cursor.execute(
                f"UPDATE {handler.database} SET {new_body} WHERE id = {result}"
            )
            if not server.cursor.rowcount:
                return Response(status=404)
        except:
            return Response(status=409)

        server.connection.commit()
        return Response(status=200)

    else:
        try:
            # Se efectueaza cererea
            server.cursor.execute(f"DELETE FROM {handler.database} WHERE id = {result}")
            if not server.cursor.rowcount:
                return Response(status=404)
        except:
            return Response(status=409)

        server.connection.commit()
        return Response(status=200)


# Clasa care modeleaza server-ul


class API_SERVER:
    def __init__(
        self,
        app: Flask,
        blueprints_lst,
        configuration,
        connection=None,
        cursor=None,
        host="0.0.0.0",
    ):
        self.app = app
        self.host = host
        self.blueprints_lst = blueprints_lst
        self.configuration = configuration
        self.connection = connection
        self.cursor = cursor

    def config_server(self):
        self.connection = MySQLdb.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            passwd=config["password"],
            db=config["database"],
        )
        self.cursor = self.connection.cursor()

    def register_blueprints(self):
        for blueprint in self.blueprints_lst:
            self.app.register_blueprint(blueprint)


# Clasa care modeleaza tabela / rutele cu tarile


class API_COUNTRIES:
    def __init__(self, blueprint, database, fields_list, request=None):
        self.database = database
        self.request = request
        self.fields_list = fields_list
        self.blueprint = blueprint

    # Se folosesc functiile generice pentru acest caz specific

    def process_put_del_pair_countries(self, id):
        return generic_process_put_del_pair(self, id)

    def process_post_get_pair_countries(self):
        return generic_process_post_get_pair(self)


# Clasa care modeleaza tabela / rutele cu orasele


class API_CITIES:
    def __init__(self, blueprint, database, fields_list, request=None):
        self.database = database
        self.request = request
        self.fields_list = fields_list
        self.blueprint = blueprint

    # Se folosesc functiile generice pentru acest caz specific

    def process_put_del_pair_cities(self, id):
        return generic_process_put_del_pair(self, id)

    def process_post_get_pair_cities(self):
        return generic_process_post_get_pair(self)

    def process_get_by_country_id_cities(self, id):
        try:
            result = int(id)
        except ValueError:
            return Response(
                status=200, response=json.dumps([]), mimetype="application/json"
            )

        # Se obtin orasele pe baza id-ului tarii

        country_id = "idTara"
        server.cursor.execute(
            f"SELECT * FROM {self.database} WHERE {country_id} = {result}"
        )
        body_info = []
        for elem in server.cursor.fetchall():
            aux_dic = {}
            for iter in range(len(self.fields_list)):
                aux_dic.update({self.fields_list[iter][0]: elem[iter]})
            body_info.append(aux_dic)
        return Response(
            status=200, response=json.dumps(body_info), mimetype="application/json"
        )


# Clasa care modeleaza tabela / rutele cu temperaturile


class API_TEMPERATURES:
    def __init__(self, blueprint, database, fields_list, request=None):
        self.database = database
        self.request = request
        self.fields_list = fields_list
        self.blueprint = blueprint

    # Se folosesc functiile generice pentru acest caz specific

    def process_put_del_pair_temperatures(self, id):
        return generic_process_put_del_pair(self, id)

    def process_post_temperatures(self):
        return generic_process_post_get_pair(self)

    # Functie care returneaza temperaturi pe baza valorilor din ruta

    def process_get_temperatures(self):
        if ("id", [int]) not in self.fields_list:
            self.fields_list.insert(0, ("id", [int]))

        lon = self.request.args.get("lon", type=float)
        lat = self.request.args.get("lat", type=float)
        lower_bound = datetime.strptime(
            self.request.args.get("from", default="1000-01-01", type=str),
            "%Y-%m-%d",
        )
        upper_bound = datetime.strptime(
            self.request.args.get("until", default="9999-12-31", type=str),
            "%Y-%m-%d",
        )

        sub_query = ""
        info = []
        if lon:
            info.append("{} = {}".format("lon", lon))
        if lat:
            info.append("{} = {}".format("lat", lat))

        if info:
            sub_query = (
                " AND {} in (SELECT {} FROM {} WHERE " + " AND ".join(info) + ")"
            ).format(self.fields_list[2][0], self.fields_list[0][0], "Orase")

        server.cursor.execute(
            (
                """SELECT {}, {}, DATE_FORMAT({}, "%Y-%m-%d")
            FROM {} WHERE {} BETWEEN '{}' AND '{}'
            {}"""
            ).format(
                self.fields_list[0][0],
                self.fields_list[1][0],
                "timestamp",
                self.database,
                "timestamp",
                lower_bound,
                upper_bound,
                sub_query,
            )
        )
        aux_lst = [self.fields_list[0][0], self.fields_list[1][0], "timestamp"]
        body_info = []

        for elem in server.cursor.fetchall():
            aux_dic = {}
            for iter in range(len(aux_lst)):
                aux_dic.update({aux_lst[iter]: elem[iter]})
            body_info.append(aux_dic)

        if body_info and len(body_info) == 3:
            if list(body_info[1].keys())[0] == "timestamp":
                body_info[1], body_info[2] = body_info[2], body_info[1]
        return Response(
            status=200, response=json.dumps(body_info), mimetype="application/json"
        )

    # Functie care returneaza temperaturi pe baza id-ului orasului

    def process_get_temperatures_by_cities_id(self, id):
        if ("id", [int]) not in self.fields_list:
            self.fields_list.insert(0, ("id", [int]))

        lower_bound = datetime.strptime(
            self.request.args.get("FROM", default="1000-01-01", type=str),
            "%Y-%m-%d",
        )
        upper_bound = datetime.strptime(
            self.request.args.get("UNTIL", default="9999-12-31", type=str),
            "%Y-%m-%d",
        )

        try:
            var = int(id)
        except:
            return Response(
                status=200, response=json.dumps([]), mimetype="application/json"
            )

        server.cursor.execute(
            (
                """SELECT {}, {}, DATE_FORMAT({}, "%Y-%m-%d")
            FROM {} WHERE
            {} = {} AND {} BETWEEN '{}' AND '{}'
            """
            ).format(
                self.fields_list[0][0],
                self.fields_list[1][0],
                "timestamp",
                self.database,
                self.fields_list[2][0],
                id,
                "timestamp",
                lower_bound,
                upper_bound,
            )
        )

        aux_lst = [self.fields_list[0][0], self.fields_list[1][0], "timestamp"]
        body_info = []

        for elem in server.cursor.fetchall():
            aux_dic = {}
            for iter in range(len(aux_lst)):
                aux_dic.update({aux_lst[iter]: elem[iter]})
            body_info.append(aux_dic)

        if body_info and len(body_info) == 3:
            if list(body_info[1].keys())[0] == "timestamp":
                body_info[1], body_info[2] = body_info[2], body_info[1]
        return Response(
            status=200, response=json.dumps(body_info), mimetype="application/json"
        )

    # Functie care returneaza temperaturi pe baza id-ului tarii

    def process_get_temperatures_by_countries_id(self, id):
        if ("id", [int]) not in self.fields_list:
            self.fields_list.insert(0, ("id", [int]))

        lower_bound = datetime.strptime(
            self.request.args.get("FROM", default="1000-01-01", type=str),
            "%Y-%m-%d",
        )
        upper_bound = datetime.strptime(
            self.request.args.get("UNTIL", default="9999-12-31", type=str),
            "%Y-%m-%d",
        )
        try:
            var = int(id)
        except:
            return Response(
                status=200, response=json.dumps([]), mimetype="application/json"
            )

        sub_query = (" AND {} in (SELECT {} FROM {} WHERE {} = {})").format(
            self.fields_list[2][0], self.fields_list[0][0], "Orase", "idTara", id
        )

        server.cursor.execute(
            (
                """SELECT {}, {}, DATE_FORMAT({}, "%Y-%m-%d")
            FROM {} WHERE {} BETWEEN '{}' AND '{}'
            {}"""
            ).format(
                self.fields_list[0][0],
                self.fields_list[1][0],
                "timestamp",
                self.database,
                "timestamp",
                lower_bound,
                upper_bound,
                sub_query,
            )
        )

        aux_lst = [self.fields_list[0][0], self.fields_list[1][0], "timestamp"]
        body_info = []

        for elem in server.cursor.fetchall():
            aux_dic = {}
            for iter in range(len(aux_lst)):
                aux_dic.update({aux_lst[iter]: elem[iter]})
            body_info.append(aux_dic)

        if body_info and len(body_info) == 3:
            if list(body_info[1].keys())[0] == "timestamp":
                body_info[1], body_info[2] = body_info[2], body_info[1]
        return Response(
            status=200, response=json.dumps(body_info), mimetype="application/json"
        )


# Mici configuratii pentru buna functionare a server-ului

app = Flask(__name__)

config = {
    "user": getenv("MYSQL_USER"),
    "password": getenv("MYSQL_PASSWORD"),
    "host": "mysql-db",
    "port": int(getenv("MYSQL_PORT")),
    "database": getenv("MYSQL_DATABASE"),
}

countries_logic = Blueprint("countries_logic", __name__)
countries = API_COUNTRIES(
    countries_logic,
    "Tari",
    [("id", [int]), ("nume", [str]), ("lat", [float, int]), ("lon", [float, int])],
)

cities_logic = Blueprint("cities_logic", __name__)
cities = API_CITIES(
    cities_logic,
    "Orase",
    [
        ("idTara", [int]),
        ("id", [int]),
        ("nume", [str]),
        ("lat", [float, int]),
        ("lon", [float, int]),
    ],
)

temperatures_logic = Blueprint("temperatures_logic", __name__)
temperatures = API_TEMPERATURES(
    temperatures_logic,
    "Temperaturi",
    [("id", [int]), ("valoare", [float, int]), ("idOras", [int])],
)

server = API_SERVER(
    app,
    [
        cities_logic,
        temperatures_logic,
        countries_logic,
    ],
    config,
)

# Functiile care modeleaza rutele tarilor


@countries_logic.route("/api/countries", methods=["GET", "POST"])
@countries_logic.route("/api/countries/<id>", methods=["DELETE", "PUT"])
def process_countries_request(id=None):
    countries.request = request
    server.config_server()

    if countries.request.method == "DELETE" or countries.request.method == "PUT":
        return countries.process_put_del_pair_countries(id)

    return countries.process_post_get_pair_countries()


# Functiile care modeleaza rutele oraselor


@cities_logic.route("/api/cities/<id>", methods=["DELETE", "PUT"])
@cities_logic.route("/api/cities", methods=["GET", "POST"])
def process_cities_request(id=None):
    cities.request = request
    server.config_server()

    if cities.request.method == "DELETE" or cities.request.method == "PUT":
        return cities.process_put_del_pair_cities(id)

    return cities.process_post_get_pair_cities()


@cities_logic.route("/api/cities/country/", methods=["GET"])
@cities_logic.route("/api/cities/country/<id>", methods=["GET"])
def process_countries_by_id_cities(id=None):
    cities.request = request
    server.config_server()

    return cities.process_get_by_country_id_cities(id)


# Functiile care modeleaza rutele temperaturilor


@temperatures_logic.route("/api/temperatures", methods=["POST"])
@temperatures_logic.route("/api/temperatures/<id>", methods=["DELETE", "PUT"])
def process_temperatures_request(id=None):
    temperatures.request = request
    server.config_server()
    global t0, t1

    if temperatures.request.method == "DELETE" or temperatures.request.method == "PUT":
        return temperatures.process_put_del_pair_temperatures(id)

    t1 = t0
    while t1 == t0:
        t1 = time.localtime().tm_sec / 10**3
    t0 = t1

    return temperatures.process_post_temperatures()


@temperatures_logic.route("/api/temperatures", methods=["GET"])
def process_temperatures_get_request():
    temperatures.request = request
    server.config_server()

    return temperatures.process_get_temperatures()


@temperatures_logic.route("/api/temperatures/cities/<id>", methods=["GET"])
def process_temperatures_get_request_by_cities(id=None):
    temperatures.request = request
    server.config_server()

    return temperatures.process_get_temperatures_by_cities_id(id)


@temperatures_logic.route("/api/temperatures/countries/<id>", methods=["GET"])
def process_temperatures_get_request_by_countries(id=None):
    temperatures.request = request
    server.config_server()

    return temperatures.process_get_temperatures_by_countries_id(id)


server.register_blueprints()

if __name__ == "__main__":
    server.app.run(server.host)
