
# Importamos bibliotecas
import pandas as pd
from inline_sql import sql, sql_val
import duckdb as ddb
import copy
import xml.etree.ElementTree as ET
import pprint

#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

equipos_=pd.read_csv("tablas_consigna/enunciado_equipos.csv")
atributos_=pd.read_csv("tablas_consigna/enunciado_jugadores_atributos.csv")
jugadores_=pd.read_csv("tablas_consigna/enunciado_jugadores.csv")
liga_=pd.read_csv("tablas_consigna/enunciado_liga.csv")
paises_=pd.read_csv("tablas_consigna/enunciado_paises.csv")
partidos_=pd.read_csv("tablas_consigna/enunciado_partidos.csv")

con = ddb.connect()

con.register('equipos_', equipos_)
con.register('atributos_', atributos_)
con.register('jugadores_', jugadores_)
con.register('liga_', liga_)
con.register('paises_', paises_)
con.register('partidos_', partidos_)

#%%===========================================================================
# Creacion de tablas adicionales
#=============================================================================

#%% ENTIDAD PAISES
con.execute("CREATE TABLE paises AS SELECT * FROM paises_")

#%% ENTIDAD LIGA
con.execute("CREATE TABLE liga AS SELECT name, country_id AS id FROM liga_")

#%% ENTIDAD PARTIDOS
# Nos quedamos con los partidos de nuestro pais asignado
con.execute("CREATE TABLE partidos AS SELECT * FROM partidos_")

#Borramos columnas innecesarias
columns_to_drop = ["shoton", "shotoff", "foulcommit", "card", "corner", "possession", "country_id", "stage"]

for column in columns_to_drop:
    con.execute(f"ALTER TABLE partidos DROP COLUMN {column}")

con.execute('ALTER TABLE partidos DROP COLUMN "cross"')

con.sql('DESCRIBE partidos').show()

#%% RELACION JUEGA
con.execute("CREATE TABLE juega AS SELECT DISTINCT match_api_id, home_team_api_id, away_team_api_id FROM partidos ORDER BY match_api_id DESC")

columns_to_drop = ["home_team_api_id", "away_team_api_id"]

for column in columns_to_drop:
    con.execute(f"ALTER TABLE partidos DROP COLUMN {column}")

#%% ENTIDAD EQUIPOS
con.execute("""CREATE TABLE equipos AS SELECT DISTINCT team_api_id, team_long_name, team_short_name
           FROM equipos_ WHERE team_api_id = (SELECT DISTINCT home_team_api_id FROM juega
           WHERE home_team_api_id = team_api_id)""")

#%% RELACION PARTICIPAN
con.execute("""CREATE TABLE participan AS SELECT DISTINCT match_api_id, home_player_1, home_player_2, 
                home_player_3, home_player_4, home_player_5, home_player_6, home_player_7, home_player_8,
                home_player_9, home_player_10, home_player_11, away_player_1, away_player_2, away_player_3,
                away_player_4, away_player_5, away_player_6, away_player_7, away_player_8, away_player_9,
                away_player_10, away_player_11 FROM partidos ORDER BY match_api_id DESC""")

#Borramos los atributos de la entidad partidos para que queden solo en la relacion
columns_to_drop = [
    "home_player_1", "home_player_2", "home_player_3", "home_player_4", "home_player_5",
    "home_player_6", "home_player_7", "home_player_8", "home_player_9", "home_player_10", "home_player_11",
    "away_player_1", "away_player_2", "away_player_3", "away_player_4", "away_player_5",
    "away_player_6", "away_player_7", "away_player_8", "away_player_9", "away_player_10", "away_player_11"
]

for column in columns_to_drop:
    con.execute(f"ALTER TABLE partidos DROP COLUMN {column}")

#%% ENTIDAD JUGADORES EN UN PLANTEL

# con.execute("DROP TABLE temp_table")
# con.execute("DROP TABLE jugadores_en_un_plantel")
# con.sql("DESCRIBE partidos")

con.execute("""CREATE TABLE temp_table (id_j_en_plantel BIGINT UNIQUE, id_equipo BIGINT, id_jugadores BIGINT, season VARCHAR)""")

con.execute("""
            CREATE TABLE equipo_partidos_jugadores_home AS
            SELECT DISTINCT 
            team_api_id, partidos.match_api_id, partidos.season, home_player_1, home_player_2, 
            home_player_3, home_player_4, home_player_5, home_player_6, home_player_7, home_player_8,
            home_player_9, home_player_10, home_player_11
            FROM equipos
            JOIN juega ON juega.home_team_api_id = equipos.team_api_id
            JOIN partidos ON partidos.match_api_id = juega.match_api_id
            JOIN participan ON participan.match_api_id = partidos.match_api_id
            """)

con.execute("""
            CREATE TABLE equipo_partidos_jugadores_away AS
            SELECT DISTINCT 
            team_api_id, partidos.match_api_id, partidos.season, away_player_1, away_player_2, away_player_3,
            away_player_4, away_player_5, away_player_6, away_player_7, away_player_8, away_player_9,
            away_player_10, away_player_11
            FROM equipos
            JOIN juega ON juega.away_team_api_id = equipos.team_api_id
            JOIN partidos ON partidos.match_api_id = juega.match_api_id
            JOIN participan ON participan.match_api_id = partidos.match_api_id
            """)

columns_insert = [
    "home_player_1", "home_player_2", "home_player_3", "home_player_4", "home_player_5",
    "home_player_6", "home_player_7", "home_player_8", "home_player_9", "home_player_10", "home_player_11"
]

for col in columns_insert:
    con.execute(f"""
                INSERT INTO temp_table (id_equipo, id_jugadores, season)
                SELECT DISTINCT team_api_id, {col}, equipo_partidos_jugadores_home.season
                FROM equipo_partidos_jugadores_home WHERE {col} IS NOT NULL
                """)

columns_insert = [
    "away_player_1", "away_player_2", "away_player_3", "away_player_4", "away_player_5",
    "away_player_6", "away_player_7", "away_player_8", "away_player_9", "away_player_10", "away_player_11"
]

for col in columns_insert:
    con.execute(f"""
                INSERT INTO temp_table (id_equipo, id_jugadores, season) 
                SELECT DISTINCT team_api_id, {col}, equipo_partidos_jugadores_away.season 
                FROM equipo_partidos_jugadores_away WHERE {col} IS NOT NULL
                """)

con.execute("""CREATE TABLE jugadores_en_un_plantel AS SELECT DISTINCT * FROM temp_table""")

# Crear una tabla temporal con IDs únicos basados en las filas de 'jugadores_en_un_plantel'
con.execute("""
            CREATE TABLE temp_ids AS
            SELECT row_number() OVER () AS id_j_en_plantel, id_equipo, id_jugadores, season
            FROM jugadores_en_un_plantel
            """)

# Actualizar la tabla original 'jugadores_en_un_plantel' con los IDs únicos de 'temp_ids'
con.execute("""
            UPDATE jugadores_en_un_plantel
            SET id_j_en_plantel = temp_ids.id_j_en_plantel
            FROM temp_ids
            WHERE jugadores_en_un_plantel.id_equipo = temp_ids.id_equipo
            AND jugadores_en_un_plantel.id_jugadores = temp_ids.id_jugadores
            AND jugadores_en_un_plantel.season = temp_ids.season
            """)
            
# con.sql("SELECT DISTINCT * FROM jugadores_en_un_plantel ORDER BY id_j_en_plantel").show()
# con.sql("SELECT * FROM jugadores_en_un_plantel ORDER BY id_j_en_plantel").show()

# Vamos a cambiar los id_jugadores de participan por id_j_de_plantel
# Agregar todas las columnas necesarias (home_J_plantel_1 a home_J_plantel_11)
# Ejecutar un UPDATE para cada jugador (home_player_1 a home_player_11 y home_J_plantel_1 a home_J_plantel_11)

con.execute("""
            DROP TABLE participan
            """)

#Reconstruimos participan de esta manera. Ya revisamos que la tabla anterior y esta creada dan el mismo resultado.

con.execute("""
            CREATE TABLE participan AS (
                SELECT equipo_partidos_jugadores_home.team_api_id AS home_team_id, 
                equipo_partidos_jugadores_away.team_api_id AS away_team_id,
                equipo_partidos_jugadores_home.match_api_id, home_player_1, home_player_2, 
                home_player_3, home_player_4, home_player_5, home_player_6, home_player_7, home_player_8,
                home_player_9, home_player_10, home_player_11, away_player_1, away_player_2, away_player_3,
                away_player_4, away_player_5, away_player_6, away_player_7, away_player_8, away_player_9,
                away_player_10, away_player_11
                FROM equipo_partidos_jugadores_home
                JOIN equipo_partidos_jugadores_away ON equipo_partidos_jugadores_away.match_api_id = equipo_partidos_jugadores_home.match_api_id
                ORDER BY equipo_partidos_jugadores_home.match_api_id DESC
                )
            """)

for i in range(1, 12):
    con.execute(f"""
            ALTER TABLE participan ADD COLUMN home_J_plantel_{i} BIGINT
            """)
    con.execute(f"""
            ALTER TABLE participan ADD COLUMN away_J_plantel_{i} BIGINT
            """)
    con.execute(f"""
            UPDATE participan 
            SET home_J_plantel_{i} = subquery.id_j_en_plantel
            FROM (SELECT * FROM jugadores_en_un_plantel) AS subquery
            WHERE participan.home_team_id = subquery.id_equipo 
            AND participan.home_player_{i} = subquery.id_jugadores
            """)
    con.execute(f"""
            UPDATE participan 
            SET away_J_plantel_{i} = subquery.id_j_en_plantel
            FROM (SELECT * FROM jugadores_en_un_plantel) AS subquery
            WHERE participan.away_team_id = subquery.id_equipo 
            AND participan.away_player_{i} = subquery.id_jugadores
            """)
    con.execute(f"""
            ALTER TABLE participan DROP COLUMN home_player_{i}
            """)
    con.execute(f"""
            ALTER TABLE participan DROP COLUMN away_player_{i}
            """)

con.execute("""
        ALTER TABLE participan DROP COLUMN home_team_id
        """)
con.execute("""
        ALTER TABLE participan DROP COLUMN away_team_id
        """)

# con.sql("SELECT DISTINCT * FROM atributos").show()

#%% RELACION TEMPORALIDAD J DE UN PLANTEL
con.sql("CREATE TABLE temporalidad_j_de_un_plantel AS (SELECT season, id_j_en_plantel FROM jugadores_en_un_plantel)")

con.execute("ALTER TABLE jugadores_en_un_plantel DROP COLUMN season")

con.sql("SELECT * FROM jugadores_en_un_plantel")
#%% ENTIDAD JUGADORES

con.execute("""
            CREATE TABLE jugadores AS (
                SELECT DISTINCT * FROM jugadores_ WHERE 
                player_api_id = (SELECT DISTINCT player_api_id FROM jugadores_en_un_plantel
                WHERE jugadores_en_un_plantel.id_jugadores = jugadores_.player_api_id)
            )
            """)

#%% ENTIDAD ATRIBUTOS
con.execute("""
            CREATE TABLE atributos AS (
                SELECT DISTINCT * FROM atributos_ WHERE 
                player_api_id = (SELECT DISTINCT player_api_id FROM jugadores
                WHERE jugadores.player_api_id = atributos_.player_api_id)
            )
            """)

con.execute("""
        ALTER TABLE atributos DROP COLUMN player_fifa_api_id
        """)
#Agrego un id para cada atributo (inicialmente como NULLs)
con.execute("""
            CREATE TABLE atributos_temp AS
            SELECT NULL AS atributos_id, * 
            FROM atributos;
            """)

# Eliminar la tabla original
con.execute("DROP TABLE atributos")

# Renombrar la tabla nueva con el nombre original
con.execute("ALTER TABLE atributos_temp RENAME TO atributos")

df = con.execute("SELECT * FROM atributos LIMIT 1").fetchdf()
columnas = list(df.columns)

# Construir la lista de columnas para el SELECT
# Esto seleccionará todas las columnas de la tabla original
select_columns = ", ".join(columnas)

# Crear una tabla temporal con IDs únicos basados en las filas de 'jugadores_en_un_plantel'
con.execute("""
            DROP TABLE temp_ids
            """)

con.execute(f"""
            CREATE TABLE temp_ids AS
            SELECT row_number() OVER () AS {select_columns}
            FROM atributos
            """)

con.sql("SELECT * FROM atributos").show()

# Actualizar la tabla original 'jugadores_en_un_plantel' con los IDs únicos de 'temp_ids'
con.execute("""
            UPDATE atributos
            SET atributos_id = temp_ids.atributos_id
            FROM temp_ids
            WHERE atributos.player_api_id = temp_ids.player_api_id
            AND atributos.date = temp_ids.date
            """)

con.sql("DESCRIBE atributos").show()

# Agrego en que temporada esta basandome en la fecha de cuando fue medido
con.execute("""ALTER TABLE atributos ADD COLUMN season VARCHAR""")

atributos_temp = con.sql(""" SELECT * FROM atributos""").to_df()

def date_to_season(date):
    año=''
    for i in list(date)[2:4]:
        año=año+i
    año=int(año)
    out='20'+f"{año:02d}"+'/20'+f"{año+1:02d}"
    return out

# Aplicar la función a la columna 'fecha'
atributos_temp['season'] = atributos_temp['date'].apply(date_to_season)

con.register('atributos_temp', atributos_temp)  # Registrar el DataFrame como tabla en DuckDB
con.execute("""DROP TABLE atributos""")
con.execute("CREATE TABLE atributos AS SELECT * FROM atributos_temp")

#%% ENTIDAD TEMPORADA
con.sql("CREATE TABLE temporada_temp AS SELECT DISTINCT season FROM atributos ORDER BY season")

con.execute("""
            CREATE TABLE temporada AS
            SELECT NULL AS id_temporada, * 
            FROM temporada_temp;
            """)

# Eliminar la tabla original
con.execute("DROP TABLE temporada_temp")

con.sql("SELECT * FROM temporada")

con.execute("DROP TABLE temp_ids")

# Crear una tabla temporal con IDs únicos basados en las filas de 'temporada'
con.execute("""
            CREATE TABLE temp_ids AS
            SELECT row_number() OVER () AS id_temporada, season
            FROM temporada
            """)

# Actualizar la tabla original 'temporada' con los IDs únicos de 'temp_ids'
con.execute("""
            UPDATE temporada
            SET id_temporada = temp_ids.id_temporada
            FROM temp_ids
            WHERE temporada.season = temp_ids.season
            """)
#Actualizo las tablas atributos jugadores, temporalidad j de un plantel y partidos, y les sacamos las fechas (cuando las tienen)
con.execute("""
            UPDATE atributos
            SET season = temporada.id_temporada
            FROM temporada
            WHERE temporada.season = atributos.season
            """)
con.execute("ALTER TABLE atributos DROP COLUMN date")

con.execute("""
            UPDATE temporalidad_j_de_un_plantel
            SET season = temporada.id_temporada
            FROM temporada
            WHERE temporada.season = temporalidad_j_de_un_plantel.season
            """)
con.execute("""
            ALTER TABLE temporalidad_j_de_un_plantel
            RENAME COLUMN season TO id_temporada
            """)

con.execute("""
            UPDATE partidos
            SET season = temporada.id_temporada
            FROM temporada
            WHERE temporada.season = partidos.season
            """)
con.execute("ALTER TABLE partidos DROP COLUMN date")
con.execute("""
            ALTER TABLE partidos
            RENAME COLUMN season TO id_temporada
            """)


con.sql("SELECT * FROM partidos")

#%% ENTIDAD GOLES
partidos = con.sql("SELECT * FROM partidos").to_df()

# Función para convertir XML a diccionario
def xml_to_dict(element):
    result = {}
    if element.tag == "goal":
        result["values"] = []  # Inicializa la lista de valores
        for value in element.findall('value'):
            value_dict = xml_to_dict(value)  # Convierte cada valor a dict
            result["values"].append(value_dict)  # Agrega el dict a la lista
    else:
        for child in element:
            # Llamar recursivamente si hay más hijos
            result[child.tag] = xml_to_dict(child) if len(child) > 0 else child.text
    return result


data = {
    "id": [],
    "minuto": [],
    "player_api_id": [],
    "match_api_id": [],
    "team_api_id": []
}

for i in range(len(partidos["match_api_id"])):
    if partidos["goal"][i] is None:
        continue
    else:
        root = ET.fromstring(partidos["goal"][i])
        goal_dict = xml_to_dict(root)
        for j in range(len(goal_dict['values'])):        
            data["id"].append(int(goal_dict['values'][j]['id']))
            data["minuto"].append(int(goal_dict['values'][j]['elapsed']))
            if 'player1' not in goal_dict['values'][j].keys():
                data["player_api_id"].append(None)
            else:
                data["player_api_id"].append(goal_dict['values'][j]['player1'])
            data["match_api_id"].append(partidos["match_api_id"][i])
            if 'team' not in goal_dict['values'][j].keys():
                data["team_api_id"].append(None)
            else:
                data["team_api_id"].append(goal_dict['values'][j]['team'])

df = pd.DataFrame(data)

con.register('goles_', df)


# con.sql("""DROP TABLE goles""")

con.sql("""CREATE TABLE goles AS 
        SELECT id AS "id_goles", minuto, goles_.match_api_id, jugadores_en_un_plantel.id_j_en_plantel FROM goles_
        JOIN partidos ON partidos.match_api_id = goles_.match_api_id
        JOIN equipos ON equipos.team_api_id = goles_.team_api_id
        JOIN jugadores_en_un_plantel ON (jugadores_en_un_plantel.id_equipo = goles_.team_api_id
                                               AND jugadores_en_un_plantel.id_jugadores = goles_.player_api_id)
        JOIN temporalidad_j_de_un_plantel ON (temporalidad_j_de_un_plantel.id_j_en_plantel = jugadores_en_un_plantel.id_j_en_plantel
                                               AND temporalidad_j_de_un_plantel.id_temporada = partidos.id_temporada)
        ORDER BY id_goles ASC
        """)
#Esta lista es mas corta que la de goles_. Esto se debe a que algunos goles no tenian asignado goleador. Entonces
#cuando hacemos el JOIN ON con player_api_id, esos desaparecen.


#%% Convert to dataframe
paises = con.sql("SELECT * FROM paises").to_df()

liga = con.sql("SELECT * FROM liga").to_df()

partidos=con.sql("SELECT * FROM partidos").to_df()

equipos=con.sql( """SELECT * FROM equipos""").to_df()

juega = con.sql("SELECT * FROM juega").to_df()

participan = con.sql(""" SELECT * FROM participan""").to_df()

plantel = con.sql(""" SELECT * FROM jugadores_en_un_plantel""").to_df()

jugadores = con.sql(""" SELECT * FROM jugadores""").to_df()

atributos = con.sql(""" SELECT * FROM atributos""").to_df()

temporalidad_j_de_un_plantel = con.sql(""" SELECT * FROM temporalidad_j_de_un_plantel""").to_df()

temporada = con.sql(""" SELECT * FROM temporada""").to_df()

goles = con.sql(""" SELECT * FROM goles""").to_df()

#%% SAVE TO CSV

paises.to_csv('tablas_todos_los_paises/paises.csv', index=False)

liga.to_csv('tablas_todos_los_paises/liga.csv', index=False)

partidos.to_csv('tablas_todos_los_paises/partidos.csv', index=False)

juega.to_csv('tablas_todos_los_paises/juega.csv', index=False)

equipos.to_csv('tablas_todos_los_paises/equipos.csv', index=False)

participan.to_csv('tablas_todos_los_paises/participan.csv', index=False)

plantel.to_csv('tablas_todos_los_paises/plantel.csv', index=False)

jugadores.to_csv('tablas_todos_los_paises/jugadores.csv', index=False)

atributos.to_csv('tablas_todos_los_paises/atributos.csv', index=False)

temporalidad_j_de_un_plantel.to_csv('tablas_todos_los_paises/temporalidad_j_de_un_plantel.csv', index=False)

temporada.to_csv('tablas_todos_los_paises/temporada.csv', index=False)

goles.to_csv('tablas_todos_los_paises/goles.csv', index=False)

#%% Borro la base de datos anterior y solo me fijo en España

con = ddb.connect()

con.register('equipos_', equipos_)
con.register('atributos_', atributos_)
con.register('jugadores_', jugadores_)
con.register('liga_', liga_)
con.register('paises_', paises_)
con.register('partidos_', partidos_)

#%%===========================================================================


# De aca en adelante, solo España


#=============================================================================
#%% ENTIDAD PAISES
con.execute("CREATE TABLE paises_spain AS SELECT * FROM paises_ WHERE name = 'Spain'")

#%% ENTIDAD LIGA
con.execute("CREATE TABLE liga_spain AS SELECT name, country_id AS id FROM liga_ WHERE country_id = (SELECT id FROM paises_spain)")

#%% ENTIDAD PARTIDOS
# Nos quedamos con los partidos de nuestro pais asignado
con.execute("CREATE TABLE partidos_spain AS SELECT * FROM partidos_ WHERE league_id = (SELECT id FROM paises_spain)") #El id de paises es = al 
#id de liga

#Borramos columnas innecesarias
columns_to_drop = ["shoton", "shotoff", "foulcommit", "card", "corner", "possession", "country_id", "stage"]

for column in columns_to_drop:
    con.execute(f"ALTER TABLE partidos_spain DROP COLUMN {column}")

con.execute('ALTER TABLE partidos_spain DROP COLUMN "cross"')

con.sql('DESCRIBE partidos_spain').show()

#%% RELACION JUEGA
con.execute("CREATE TABLE juega_spain AS SELECT DISTINCT match_api_id, home_team_api_id, away_team_api_id FROM partidos_spain ORDER BY match_api_id DESC")

columns_to_drop = ["home_team_api_id", "away_team_api_id"]

for column in columns_to_drop:
    con.execute(f"ALTER TABLE partidos_spain DROP COLUMN {column}")

#%% ENTIDAD EQUIPOS
con.execute("""CREATE TABLE equipos_spain AS SELECT DISTINCT team_api_id, team_long_name, team_short_name
           FROM equipos_ WHERE team_api_id = (SELECT DISTINCT home_team_api_id FROM juega_spain
           WHERE home_team_api_id = team_api_id)""")

#%% RELACION PARTICIPAN
con.execute("""CREATE TABLE participan_spain AS SELECT DISTINCT match_api_id, home_player_1, home_player_2, 
                home_player_3, home_player_4, home_player_5, home_player_6, home_player_7, home_player_8,
                home_player_9, home_player_10, home_player_11, away_player_1, away_player_2, away_player_3,
                away_player_4, away_player_5, away_player_6, away_player_7, away_player_8, away_player_9,
                away_player_10, away_player_11 FROM partidos_spain ORDER BY match_api_id DESC""")

#Borramos los atributos de la entidad partidos para que queden solo en la relacion
columns_to_drop = [
    "home_player_1", "home_player_2", "home_player_3", "home_player_4", "home_player_5",
    "home_player_6", "home_player_7", "home_player_8", "home_player_9", "home_player_10", "home_player_11",
    "away_player_1", "away_player_2", "away_player_3", "away_player_4", "away_player_5",
    "away_player_6", "away_player_7", "away_player_8", "away_player_9", "away_player_10", "away_player_11"
]

for column in columns_to_drop:
    con.execute(f"ALTER TABLE partidos_spain DROP COLUMN {column}")

#%% ENTIDAD JUGADORES EN UN PLANTEL

# con.execute("DROP TABLE temp_table")
# con.execute("DROP TABLE jugadores_en_un_plantel_spain")
# con.sql("DESCRIBE partidos_spain")


con.execute("""CREATE TABLE temp_table (id_j_en_plantel BIGINT UNIQUE, id_equipo BIGINT, id_jugadores BIGINT, season VARCHAR)""")

con.execute("""
            CREATE TABLE equipo_partidos_jugadores_home_spain AS
            SELECT DISTINCT 
            team_api_id, partidos_spain.match_api_id, partidos_spain.season, home_player_1, home_player_2, 
            home_player_3, home_player_4, home_player_5, home_player_6, home_player_7, home_player_8,
            home_player_9, home_player_10, home_player_11
            FROM equipos_spain
            JOIN juega_spain ON juega_spain.home_team_api_id = equipos_spain.team_api_id
            JOIN partidos_spain ON partidos_spain.match_api_id = juega_spain.match_api_id
            JOIN participan_spain ON participan_spain.match_api_id = partidos_spain.match_api_id
            """)

con.execute("""
            CREATE TABLE equipo_partidos_jugadores_away_spain AS
            SELECT DISTINCT 
            team_api_id, partidos_spain.match_api_id, partidos_spain.season, away_player_1, away_player_2, away_player_3,
            away_player_4, away_player_5, away_player_6, away_player_7, away_player_8, away_player_9,
            away_player_10, away_player_11
            FROM equipos_spain
            JOIN juega_spain ON juega_spain.away_team_api_id = equipos_spain.team_api_id
            JOIN partidos_spain ON partidos_spain.match_api_id = juega_spain.match_api_id
            JOIN participan_spain ON participan_spain.match_api_id = partidos_spain.match_api_id
            """)

columns_insert = [
    "home_player_1", "home_player_2", "home_player_3", "home_player_4", "home_player_5",
    "home_player_6", "home_player_7", "home_player_8", "home_player_9", "home_player_10", "home_player_11"
]

for col in columns_insert:
    con.execute(f"""
                INSERT INTO temp_table (id_equipo, id_jugadores, season)
                SELECT DISTINCT team_api_id, {col}, equipo_partidos_jugadores_home_spain.season
                FROM equipo_partidos_jugadores_home_spain WHERE {col} IS NOT NULL
                """)

columns_insert = [
    "away_player_1", "away_player_2", "away_player_3", "away_player_4", "away_player_5",
    "away_player_6", "away_player_7", "away_player_8", "away_player_9", "away_player_10", "away_player_11"
]

for col in columns_insert:
    con.execute(f"""
                INSERT INTO temp_table (id_equipo, id_jugadores, season) 
                SELECT DISTINCT team_api_id, {col}, equipo_partidos_jugadores_away_spain.season 
                FROM equipo_partidos_jugadores_away_spain WHERE {col} IS NOT NULL
                """)

con.execute("""CREATE TABLE jugadores_en_un_plantel_spain AS SELECT DISTINCT * FROM temp_table""")



# Crear una tabla temporal con IDs únicos basados en las filas de 'jugadores_en_un_plantel_spain'
con.execute("""
            CREATE TABLE temp_ids AS
            SELECT row_number() OVER () AS id_j_en_plantel, id_equipo, id_jugadores, season
            FROM jugadores_en_un_plantel_spain
            """)

# Actualizar la tabla original 'jugadores_en_un_plantel_spain' con los IDs únicos de 'temp_ids'
con.execute("""
            UPDATE jugadores_en_un_plantel_spain
            SET id_j_en_plantel = temp_ids.id_j_en_plantel
            FROM temp_ids
            WHERE jugadores_en_un_plantel_spain.id_equipo = temp_ids.id_equipo
            AND jugadores_en_un_plantel_spain.id_jugadores = temp_ids.id_jugadores
            AND jugadores_en_un_plantel_spain.season = temp_ids.season
            """)
            
# con.sql("SELECT DISTINCT * FROM jugadores_en_un_plantel_spain ORDER BY id_j_en_plantel").show()
# con.sql("SELECT * FROM jugadores_en_un_plantel_spain ORDER BY id_j_en_plantel").show()

# Vamos a cambiar los id_jugadores de participan por id_j_de_plantel
# Agregar todas las columnas necesarias (home_J_plantel_1 a home_J_plantel_11)
# Ejecutar un UPDATE para cada jugador (home_player_1 a home_player_11 y home_J_plantel_1 a home_J_plantel_11)

con.execute("""
            DROP TABLE participan_spain
            """)

#Reconstruimos participan_spain de esta manera. Ya revisamos que la tabla anterior y esta creada dan el mismo resultado.

con.execute("""
            CREATE TABLE participan_spain AS (
                SELECT equipo_partidos_jugadores_home_spain.team_api_id AS home_team_id, 
                equipo_partidos_jugadores_away_spain.team_api_id AS away_team_id,
                equipo_partidos_jugadores_home_spain.match_api_id, home_player_1, home_player_2, 
                home_player_3, home_player_4, home_player_5, home_player_6, home_player_7, home_player_8,
                home_player_9, home_player_10, home_player_11, away_player_1, away_player_2, away_player_3,
                away_player_4, away_player_5, away_player_6, away_player_7, away_player_8, away_player_9,
                away_player_10, away_player_11
                FROM equipo_partidos_jugadores_home_spain
                JOIN equipo_partidos_jugadores_away_spain ON equipo_partidos_jugadores_away_spain.match_api_id = equipo_partidos_jugadores_home_spain.match_api_id
                ORDER BY equipo_partidos_jugadores_home_spain.match_api_id DESC
                )
            """)

for i in range(1, 12):
    con.execute(f"""
            ALTER TABLE participan_spain ADD COLUMN home_J_plantel_{i} BIGINT
            """)
    con.execute(f"""
            ALTER TABLE participan_spain ADD COLUMN away_J_plantel_{i} BIGINT
            """)
    con.execute(f"""
            UPDATE participan_spain 
            SET home_J_plantel_{i} = subquery.id_j_en_plantel
            FROM (SELECT * FROM jugadores_en_un_plantel_spain) AS subquery
            WHERE participan_spain.home_team_id = subquery.id_equipo 
            AND participan_spain.home_player_{i} = subquery.id_jugadores
            """)
    con.execute(f"""
            UPDATE participan_spain 
            SET away_J_plantel_{i} = subquery.id_j_en_plantel
            FROM (SELECT * FROM jugadores_en_un_plantel_spain) AS subquery
            WHERE participan_spain.away_team_id = subquery.id_equipo 
            AND participan_spain.away_player_{i} = subquery.id_jugadores
            """)
    con.execute(f"""
            ALTER TABLE participan_spain DROP COLUMN home_player_{i}
            """)
    con.execute(f"""
            ALTER TABLE participan_spain DROP COLUMN away_player_{i}
            """)

con.execute("""
        ALTER TABLE participan_spain DROP COLUMN home_team_id
        """)
con.execute("""
        ALTER TABLE participan_spain DROP COLUMN away_team_id
        """)

# con.sql("SELECT DISTINCT * FROM atributos_spain").show()

#%% RELACION TEMPORALIDAD J DE UN PLANTEL 
con.sql("CREATE TABLE temporalidad_j_de_un_plantel_spain AS (SELECT season, id_j_en_plantel FROM jugadores_en_un_plantel_spain)")

con.execute("ALTER TABLE jugadores_en_un_plantel_spain DROP COLUMN season")

con.sql("SELECT * FROM jugadores_en_un_plantel_spain")
#%% ENTIDAD JUGADORES

con.execute("""
            CREATE TABLE jugadores_spain AS (
                SELECT DISTINCT * FROM jugadores_ WHERE 
                player_api_id = (SELECT DISTINCT player_api_id FROM jugadores_en_un_plantel_spain
                WHERE jugadores_en_un_plantel_spain.id_jugadores = jugadores_.player_api_id)
            )
            """)

#%% ENTIDAD ATRIBUTOS
con.execute("""
            CREATE TABLE atributos_spain AS (
                SELECT DISTINCT * FROM atributos_ WHERE 
                player_api_id = (SELECT DISTINCT player_api_id FROM jugadores_spain
                WHERE jugadores_spain.player_api_id = atributos_.player_api_id)
            )
            """)

con.execute("""
        ALTER TABLE atributos_spain DROP COLUMN player_fifa_api_id
        """)
#Agrego un id para cada atributo (inicialmente como NULLs)
con.execute("""
            CREATE TABLE atributos_spain_temp AS
            SELECT NULL AS atributos_id, * 
            FROM atributos_spain;
            """)

# Eliminar la tabla original
con.execute("DROP TABLE atributos_spain")

# Renombrar la tabla nueva con el nombre original
con.execute("ALTER TABLE atributos_spain_temp RENAME TO atributos_spain")

df = con.execute("SELECT * FROM atributos_spain LIMIT 1").fetchdf()
columnas = list(df.columns)

# Construir la lista de columnas para el SELECT
# Esto seleccionará todas las columnas de la tabla original
select_columns = ", ".join(columnas)

# Crear una tabla temporal con IDs únicos basados en las filas de 'jugadores_en_un_plantel_spain'
con.execute("""
            DROP TABLE temp_ids
            """)

con.execute(f"""
            CREATE TABLE temp_ids AS
            SELECT row_number() OVER () AS {select_columns}
            FROM atributos_spain
            """)

con.sql("SELECT * FROM atributos_spain").show()

# Actualizar la tabla original 'jugadores_en_un_plantel_spain' con los IDs únicos de 'temp_ids'
con.execute("""
            UPDATE atributos_spain
            SET atributos_id = temp_ids.atributos_id
            FROM temp_ids
            WHERE atributos_spain.player_api_id = temp_ids.player_api_id
            AND atributos_spain.date = temp_ids.date
            """)

con.sql("DESCRIBE atributos_spain").show()

#%% RELACION TEMPORALIDAD ATRIBUTOS

con.execute("""ALTER TABLE atributos_spain ADD COLUMN season VARCHAR""")

atributos_temp = con.sql(""" SELECT * FROM atributos_spain""").to_df()

def date_to_season(date):
    año=''
    for i in list(date)[2:4]:
        año=año+i
    año=int(año)
    out='20'+f"{año:02d}"+'/20'+f"{año+1:02d}"
    return out

# Aplicar la función a la columna 'fecha'
atributos_temp['season'] = atributos_temp['date'].apply(date_to_season)

con.register('atributos_spain_temp', atributos_temp)  # Registrar el DataFrame como tabla en DuckDB
con.execute("""DROP TABLE atributos_spain""")
con.execute("CREATE TABLE atributos_spain AS SELECT * FROM atributos_spain_temp")

#%% ENTIDAD TEMPORADA
con.sql("CREATE TABLE temporada_spain_temp AS SELECT DISTINCT season FROM atributos_spain ORDER BY season")

con.execute("""
            CREATE TABLE temporada_spain AS
            SELECT NULL AS id_temporada, * 
            FROM temporada_spain_temp;
            """)

# Eliminar la tabla original
con.execute("DROP TABLE temporada_spain_temp")

con.sql("SELECT * FROM temporada_spain")

con.execute("DROP TABLE temp_ids")

# Crear una tabla temporal con IDs únicos basados en las filas de 'temporada_spain'
con.execute("""
            CREATE TABLE temp_ids AS
            SELECT row_number() OVER () AS id_temporada, season
            FROM temporada_spain
            """)

# Actualizar la tabla original 'temporada_spain' con los IDs únicos de 'temp_ids'
con.execute("""
            UPDATE temporada_spain
            SET id_temporada = temp_ids.id_temporada
            FROM temp_ids
            WHERE temporada_spain.season = temp_ids.season
            """)
#Actualizo las tablas atributos jugadores, temporalidad j de un plantel y partidos, y les sacamos las fechas (cuando las tienen)
con.execute("""
            UPDATE atributos_spain
            SET season = temporada_spain.id_temporada
            FROM temporada_spain
            WHERE temporada_spain.season = atributos_spain.season
            """)
con.execute("""
            ALTER TABLE atributos_spain
            RENAME COLUMN season TO id_temporada
            """)
con.execute("ALTER TABLE atributos_spain DROP COLUMN date")

con.execute("""
            UPDATE temporalidad_j_de_un_plantel_spain
            SET season = temporada_spain.id_temporada
            FROM temporada_spain
            WHERE temporada_spain.season = temporalidad_j_de_un_plantel_spain.season
            """)
con.execute("""
            ALTER TABLE temporalidad_j_de_un_plantel_spain
            RENAME COLUMN season TO id_temporada
            """)

con.execute("""
            UPDATE partidos_spain
            SET season = temporada_spain.id_temporada
            FROM temporada_spain
            WHERE temporada_spain.season = partidos_spain.season
            """)
con.execute("""
            ALTER TABLE partidos_spain
            RENAME COLUMN season TO id_temporada
            """)
con.execute("ALTER TABLE partidos_spain DROP COLUMN date")

#%% ENTIDAD GOLES
partidos_spain = con.sql("SELECT * FROM partidos_spain").to_df()

# Función para convertir XML a diccionario
def xml_to_dict(element):
    result = {}
    if element.tag == "goal":
        result["values"] = []  # Inicializa la lista de valores
        for value in element.findall('value'):
            value_dict = xml_to_dict(value)  # Convierte cada valor a dict
            result["values"].append(value_dict)  # Agrega el dict a la lista
    else:
        for child in element:
            # Llamar recursivamente si hay más hijos
            result[child.tag] = xml_to_dict(child) if len(child) > 0 else child.text
    return result


data = {
    "id": [],
    "minuto": [],
    "player_api_id": [],
    "match_api_id": [],
    "team_api_id": []
}

for i in range(len(partidos_spain["match_api_id"])):
    if partidos_spain["goal"][i] is None:
        continue
    else:
        root = ET.fromstring(partidos_spain["goal"][i])
        goal_dict = xml_to_dict(root)
        for j in range(len(goal_dict['values'])):        
            data["id"].append(int(goal_dict['values'][j]['id']))
            data["minuto"].append(int(goal_dict['values'][j]['elapsed']))
            if 'player1' not in goal_dict['values'][j].keys():
                data["player_api_id"].append(None)
            else:
                data["player_api_id"].append(goal_dict['values'][j]['player1'])
            data["match_api_id"].append(partidos_spain["match_api_id"][i])
            if 'team' not in goal_dict['values'][j].keys():
                data["team_api_id"].append(None)
            else:
                data["team_api_id"].append(goal_dict['values'][j]['team'])

df = pd.DataFrame(data)

con.register('goles_', df)


# con.sql("""DROP TABLE goles_spain""")

con.sql("""CREATE TABLE goles_spain AS 
        SELECT id AS "id_goles", minuto, goles_.match_api_id, jugadores_en_un_plantel_spain.id_j_en_plantel FROM goles_
        JOIN partidos_spain ON partidos_spain.match_api_id = goles_.match_api_id
        JOIN equipos_spain ON equipos_spain.team_api_id = goles_.team_api_id
        JOIN jugadores_en_un_plantel_spain ON (jugadores_en_un_plantel_spain.id_equipo = goles_.team_api_id
                                               AND jugadores_en_un_plantel_spain.id_jugadores = goles_.player_api_id)
        JOIN temporalidad_j_de_un_plantel_spain ON (temporalidad_j_de_un_plantel_spain.id_j_en_plantel = jugadores_en_un_plantel_spain.id_j_en_plantel
                                               AND temporalidad_j_de_un_plantel_spain.id_temporada = partidos_spain.id_temporada)
        ORDER BY id_goles ASC
        """)
#Esta lista es mas corta que la de goles_. Esto se debe a que algunos goles no tenian asignado goleador. Entonces
#cuando hacemos el JOIN ON con player_api_id, esos desaparecen.


#%% ############################################
#Tablas para la facilitacion de los graficos
################################################
#%% GRAFICO 1 y 2 
con.execute("""
    CREATE TABLE graficos AS
    SELECT 
        equipos_spain.team_api_id,
        temporada_spain.id_temporada,
        SUM(CASE WHEN juega_spain.home_team_api_id = equipos_spain.team_api_id THEN partidos_spain.home_team_goal 
                 WHEN juega_spain.away_team_api_id = equipos_spain.team_api_id THEN partidos_spain.away_team_goal
                 END) AS goles_afavor,
        SUM(CASE WHEN juega_spain.home_team_api_id = equipos_spain.team_api_id THEN partidos_spain.away_team_goal
                 WHEN juega_spain.away_team_api_id = equipos_spain.team_api_id THEN partidos_spain.home_team_goal
                 END) AS goles_encontra,
        AVG(CASE WHEN juega_spain.home_team_api_id = equipos_spain.team_api_id THEN partidos_spain.home_team_goal
                 WHEN juega_spain.away_team_api_id = equipos_spain.team_api_id THEN partidos_spain.away_team_goal
                 END) AS promedio_gol
    FROM 
        partidos_spain
    JOIN 
        juega_spain ON partidos_spain.match_api_id = juega_spain.match_api_id
    JOIN 
        equipos_spain ON (juega_spain.home_team_api_id = equipos_spain.team_api_id OR 
                          juega_spain.away_team_api_id = equipos_spain.team_api_id)
    JOIN 
        temporada_spain ON partidos_spain.id_temporada = temporada_spain.id_temporada
    GROUP BY 
        equipos_spain.team_api_id, temporada_spain.id_temporada
    ORDER BY equipos_spain.team_api_id, temporada_spain.id_temporada 
""")


con.execute("""
        CREATE TABLE graficos1_2 AS
        SELECT 
            graficos.team_api_id,
            graficos.id_temporada,
            graficos.goles_afavor,
            graficos.goles_encontra,
            graficos.promedio_gol,
            equipos_spain.team_long_name AS nombre_equipo,
            temporada_spain.season AS nombre_temporada
        FROM 
            graficos 
        JOIN 
            equipos_spain ON graficos.team_api_id = equipos_spain.team_api_id
        JOIN 
            temporada_spain ON graficos.id_temporada = temporada_spain.id_temporada
        WHERE 
            graficos.id_temporada BETWEEN 2 AND 5
        ORDER BY 
            equipos_spain.team_api_id, temporada_spain.id_temporada
        """)

#%% GRAFICO 3 

con.execute("""
    CREATE TABLE graficos1 AS
    SELECT 
        equipos_spain.team_api_id,
        temporada_spain.id_temporada,
        SUM(CASE WHEN juega_spain.home_team_api_id = equipos_spain.team_api_id THEN partidos_spain.home_team_goal 
                 END) AS goles_de_local,
        SUM(CASE WHEN juega_spain.away_team_api_id = equipos_spain.team_api_id THEN partidos_spain.away_team_goal
                 END) AS goles_de_visitante
    FROM 
        partidos_spain
    JOIN 
        juega_spain ON partidos_spain.match_api_id = juega_spain.match_api_id
    JOIN 
        equipos_spain ON (juega_spain.home_team_api_id = equipos_spain.team_api_id OR 
                          juega_spain.away_team_api_id = equipos_spain.team_api_id)
    JOIN 
        temporada_spain ON partidos_spain.id_temporada = temporada_spain.id_temporada
    GROUP BY 
        equipos_spain.team_api_id, temporada_spain.id_temporada
    ORDER BY equipos_spain.team_api_id, temporada_spain.id_temporada 
""")

#con.execute("DROP TABLE grafico3")

con.execute("""
        CREATE TABLE grafico3 AS
        SELECT 
            graficos1.team_api_id,
            graficos1.id_temporada,
            graficos1.goles_de_local,
            graficos1.goles_de_visitante,
            (graficos1.goles_de_local - graficos1.goles_de_visitante) AS diferencia_goles_LvsV,
            equipos_spain.team_long_name AS nombre_equipo,
            temporada_spain.season AS nombre_temporada
        FROM 
            graficos1 
        JOIN 
            equipos_spain ON graficos1.team_api_id = equipos_spain.team_api_id
        JOIN 
            temporada_spain ON graficos1.id_temporada = temporada_spain.id_temporada
        WHERE 
            graficos1.id_temporada BETWEEN 2 AND 5
        ORDER BY 
            equipos_spain.team_api_id, temporada_spain.id_temporada
        """)
        
#%% GRAFICO 4 
con.execute("""
            CREATE TABLE grafico4 AS
            SELECT 
                E.team_api_id,
                E.team_long_name,
                SUM(AJ.overall_rating) AS suma_total_overall,
                SUM(
                    CASE 
                        WHEN JG.home_team_api_id = E.team_api_id THEN P.home_team_goal
                        WHEN JG.away_team_api_id = E.team_api_id THEN P.away_team_goal
                        ELSE 0
                    END
                ) AS suma_total_goles
            FROM 
                equipos_spain AS E
            JOIN 
                jugadores_en_un_plantel_spain AS JP ON E.team_api_id = JP.id_equipo
            JOIN 
                jugadores_spain AS J ON JP.id_jugadores = J.player_api_id
            JOIN 
                atributos_spain AS AJ ON J.player_api_id = AJ.player_api_id
            JOIN 
                juega_spain AS JG ON E.team_api_id = JG.home_team_api_id OR E.team_api_id = JG.away_team_api_id
            JOIN    
                partidos_spain AS P ON JG.match_api_id = P.match_api_id
            WHERE 
                CAST(AJ.id_temporada AS INTEGER) BETWEEN 2 AND 5
                AND CAST(P.id_temporada AS INTEGER) BETWEEN 2 AND 5
            GROUP BY 
                E.team_api_id, E.team_long_name;
            """)
                        
            
#%% Convert to dataframe
paises_spain = con.sql("SELECT * FROM paises_spain").to_df()

liga_spain = con.sql("SELECT * FROM liga_spain").to_df()

partidos_spain = con.sql("SELECT * FROM partidos_spain").to_df()

equipos_spain = con.sql( """SELECT * FROM equipos_spain""").to_df()

juega_spain = con.sql("SELECT * FROM juega_spain").to_df()

participan_spain = con.sql(""" SELECT * FROM participan_spain""").to_df()

plantel_spain = con.sql(""" SELECT * FROM jugadores_en_un_plantel_spain""").to_df()

jugadores_spain = con.sql(""" SELECT * FROM jugadores_spain""").to_df()

atributos_spain = con.sql(""" SELECT * FROM atributos_spain""").to_df()

temporalidad_j_de_un_plantel_spain = con.sql(""" SELECT * FROM temporalidad_j_de_un_plantel_spain""").to_df()

temporada_spain = con.sql(""" SELECT * FROM temporada_spain""").to_df()

graficos_1_2 = con.sql("SELECT * FROM graficos1_2").to_df()

goles_spain = con.sql(""" SELECT * FROM goles_spain""").to_df()

grafico_3 = con.sql("SELECT * FROM grafico3").to_df()

grafico_4 = con.sql("SELECT * FROM grafico4").to_df()




#%% SAVE TO CSV

paises_spain.to_csv('tablas_españa/paises_spain.csv', index=False)

liga_spain.to_csv('tablas_españa/liga_spain.csv', index=False)

partidos_spain.to_csv('tablas_españa/partidos_spain.csv', index=False)

juega_spain.to_csv('tablas_españa/juega_spain.csv', index=False)

equipos_spain.to_csv('tablas_españa/equipos_spain.csv', index=False)

participan_spain.to_csv('tablas_españa/participan_spain.csv', index=False)

plantel_spain.to_csv('tablas_españa/plantel_spain.csv', index=False)

jugadores_spain.to_csv('tablas_españa/jugadores_spain.csv', index=False)

atributos_spain.to_csv('tablas_españa/atributos_spain.csv', index=False)

temporalidad_j_de_un_plantel_spain.to_csv('tablas_españa/temporalidad_j_de_un_plantel_spain.csv', index=False)

temporada_spain.to_csv('tablas_españa/temporada_spain.csv', index=False)

goles_spain.to_csv('tablas_españa/goles_spain.csv', index=False)

graficos_1_2.to_csv('tablas_graficos/graficos_1_2.csv', index=False)

grafico_3.to_csv('tablas_graficos/graficos_3.csv', index=False)

grafico_4.to_csv('tablas_graficos/graficos_4.csv', index=False)






























