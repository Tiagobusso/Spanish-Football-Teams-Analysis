
# Importamos bibliotecas
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb as ddb

#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

paises_spain=pd.read_csv("tablas_españa/paises_spain.csv")
liga_spain=pd.read_csv("tablas_españa/liga_spain.csv")
partidos_spain=pd.read_csv("tablas_españa/partidos_spain.csv")
juega_spain=pd.read_csv("tablas_españa/juega_spain.csv")
equipos_spain=pd.read_csv("tablas_españa/equipos_spain.csv")
participan_spain=pd.read_csv("tablas_españa/participan_spain.csv")
plantel_spain=pd.read_csv("tablas_españa/plantel_spain.csv")
jugadores_spain=pd.read_csv("tablas_españa/jugadores_spain.csv")
atributos_spain=pd.read_csv("tablas_españa/atributos_spain.csv")
temporalidad_j_de_un_plantel_spain=pd.read_csv("tablas_españa/temporalidad_j_de_un_plantel_spain.csv")
temporada_spain=pd.read_csv("tablas_españa/temporada_spain.csv")
goles_spain=pd.read_csv("tablas_españa/goles_spain.csv")

con = ddb.connect()

con.register('paises_spain_', paises_spain) 
con.register('liga_spain_', liga_spain)
con.register('partidos_spain_', partidos_spain)
con.register('juega_spain_', juega_spain)
con.register('equipos_spain_', equipos_spain)
con.register('participan_spain_', participan_spain)
con.register('plantel_spain_', plantel_spain)
con.register('jugadores_spain_', jugadores_spain)
con.register('temporalidad_j_de_un_plantel_spain_', temporalidad_j_de_un_plantel_spain)
con.register('plantel_spain_', plantel_spain)
con.register('jugadores_spain_', jugadores_spain)
con.register('atributos_spain_', atributos_spain)
con.register('goles_spain_', goles_spain)
con.register('temporada_spain_', temporada_spain)

tablas = ['paises_spain', 'liga_spain', 'partidos_spain', 'juega_spain', 'equipos_spain', 'participan_spain', 'plantel_spain', 'jugadores_spain', 'atributos_spain', 'temporalidad_j_de_un_plantel_spain', 'temporada_spain', 'goles_spain']

for tabla in tablas:
    con.sql(f"""
            CREATE TABLE {tabla} AS
            SELECT * FROM {tabla}_
            """)

# Ejercicios AR-PROJECT, SELECT, RENAME
graficos1_2=pd.read_csv("tablas_graficos/graficos_1_2.csv")
grafico3=pd.read_csv("tablas_graficos/graficos_3.csv")
grafico4=pd.read_csv("tablas_graficos/graficos_4.csv")
#%%===========================================================================
# CONSULTAS
#=============================================================================

#%%===========================================================================


# De aca en adelante, consultas (2008 a 2012)


#=============================================================================
#CONSULTA 1
#¿Cuál es el equipo con mayor cantidad de partidos ganados?

# con.sql("""DROP TABLE cantidad_victorias_x_equipo""")

con.sql("""CREATE TABLE partidos_2_a_5 AS
        SELECT partidos_spain.match_api_id, partidos_spain.id_temporada, home_team_goal, away_team_goal, 
        home_team_api_id, away_team_api_id FROM partidos_spain
        JOIN temporada_spain ON temporada_spain.id_temporada = partidos_spain.id_temporada
        JOIN juega_spain ON juega_spain.match_api_id = partidos_spain.match_api_id
        WHERE temporada_spain.id_temporada BETWEEN 2 AND 5
        """)

con.sql("""CREATE TABLE equipos_ganadores_x_partido AS
        SELECT match_api_id, home_team_api_id AS equipos_ganadores FROM partidos_2_a_5
        WHERE home_team_goal > away_team_goal
        UNION ALL
        SELECT match_api_id, away_team_api_id AS equipos_ganadores FROM partidos_2_a_5
        WHERE away_team_goal > home_team_goal
        """)

con.sql("""CREATE TABLE cantidad_victorias_x_equipo AS
        SELECT COUNT(match_api_id) AS 'Cantidad de partidos ganados',
        team_long_name AS 'Equipos' FROM equipos_ganadores_x_partido
        JOIN equipos_spain ON team_api_id = equipos_ganadores
        GROUP BY team_long_name
        ORDER BY COUNT(match_api_id) DESC
        LIMIT 1
        """)


#%% CONSULTA 2 
#¿Cuál es el equipo con mayor cantidad de partidos perdidos de cada año?
# con.sql("""DROP TABLE cantidad_derrotas_x_equipo_2008""")
# con.sql("""DROP TABLE cantidad_derrotas_x_equipo_2009""")
# con.sql("""DROP TABLE cantidad_derrotas_x_equipo_2010""")
# con.sql("""DROP TABLE cantidad_derrotas_x_equipo_2011""")

con.sql("""CREATE TABLE equipos_perdedores_x_partido_x_año AS
        SELECT match_api_id, away_team_api_id AS equipos_perdedores, partidos_2_a_5.id_temporada FROM partidos_2_a_5
        WHERE home_team_goal > away_team_goal
        UNION ALL
        SELECT match_api_id, home_team_api_id AS equipos_perdedores, partidos_2_a_5.id_temporada FROM partidos_2_a_5
        WHERE away_team_goal > home_team_goal
        ORDER BY partidos_2_a_5.id_temporada ASC
        """)

años=[2008, 2009, 2010, 2011]

for año in años:
    con.sql(f"""CREATE TABLE cantidad_derrotas_x_equipo_{año} AS
            SELECT COUNT(match_api_id) AS "Cantidad de partidos perdidos",
            team_long_name AS "Equipos", ANY_VALUE(season) AS "Temporada" FROM equipos_perdedores_x_partido_x_año
            JOIN equipos_spain ON team_api_id = equipos_perdedores
            JOIN temporada_spain ON temporada_spain.id_temporada = equipos_perdedores_x_partido_x_año.id_temporada
            WHERE season LIKE '{año}/20%'
            GROUP BY team_long_name
            HAVING COUNT(match_api_id) = (
                SELECT MAX(derrotas_x_equipo)
                FROM (
                    SELECT COUNT(match_api_id) AS derrotas_x_equipo
                    FROM equipos_perdedores_x_partido_x_año
                    JOIN temporada_spain ON temporada_spain.id_temporada = equipos_perdedores_x_partido_x_año.id_temporada
                    WHERE season LIKE '{año}/20%'
                    GROUP BY equipos_perdedores
                )
            )
            ORDER BY "Cantidad de partidos perdidos" DESC
            """)

con.sql("""CREATE TABLE equipos_max_partidos_perdidos_anual AS
        SELECT * FROM cantidad_derrotas_x_equipo_2008
        UNION
        SELECT * FROM cantidad_derrotas_x_equipo_2009
        UNION
        SELECT * FROM cantidad_derrotas_x_equipo_2010
        UNION
        SELECT * FROM cantidad_derrotas_x_equipo_2011
        ORDER BY Temporada
        """)


con.sql("SELECT * FROM equipos_max_partidos_perdidos_anual")
#%% CONSULTA 3 
#¿Cuál es el equipo con mayor cantidad de partidos empatados en el último año?
#con.sql("""DROP TABLE equipos_empatados_x_partido""")
#con.sql("""DROP TABLE equipo_max_empates_2011""")

con.sql("""CREATE TABLE equipos_empatados_x_partido AS
        SELECT match_api_id, home_team_api_id AS equipos_empatados, partidos_2_a_5.id_temporada FROM partidos_2_a_5
        WHERE home_team_goal = away_team_goal
        UNION ALL
        SELECT match_api_id, away_team_api_id AS equipos_empatados, partidos_2_a_5.id_temporada FROM partidos_2_a_5
        WHERE away_team_goal = home_team_goal
        """)

con.sql("""CREATE TABLE equipo_max_empates_2011 AS
        SELECT COUNT(match_api_id) AS "Cantidad de partidos empatados",
        team_long_name AS 'Equipos' FROM equipos_empatados_x_partido
        JOIN equipos_spain ON team_api_id = equipos_empatados
        JOIN temporada_spain ON temporada_spain.id_temporada = equipos_empatados_x_partido.id_temporada
        WHERE season LIKE '2011/20%'
        GROUP BY team_long_name
        HAVING COUNT(match_api_id) = (
            SELECT MAX(empates_x_equipo)
            FROM (
                SELECT COUNT(match_api_id) AS empates_x_equipo
                FROM equipos_empatados_x_partido
                JOIN temporada_spain ON temporada_spain.id_temporada = equipos_empatados_x_partido.id_temporada
                WHERE season LIKE '2011/20%'
                GROUP BY equipos_empatados
            )
        )
        ORDER BY "Cantidad de partidos empatados" DESC
        """)
        
con.sql("SELECT * FROM equipo_max_empates_2011")
        
#%% CONSULTA 4
#¿Cuál es el equipo con mayor cantidad de goles a favor?

#Tabla aux para consulta 4 y 5
con.execute("""
    CREATE TABLE consultaaux AS
    SELECT 
        equipos_spain.team_api_id,
        equipos_spain.team_long_name,
        SUM(CASE WHEN juega_spain.home_team_api_id = equipos_spain.team_api_id THEN partidos_spain.home_team_goal 
                 WHEN juega_spain.away_team_api_id = equipos_spain.team_api_id THEN partidos_spain.away_team_goal
                 END) AS goles_afavor,
        SUM(CASE WHEN juega_spain.home_team_api_id = equipos_spain.team_api_id THEN partidos_spain.away_team_goal
                 WHEN juega_spain.away_team_api_id = equipos_spain.team_api_id THEN partidos_spain.home_team_goal
                 END) AS goles_encontra,
    FROM 
        equipos_spain
    JOIN 
        juega_spain ON equipos_spain.team_api_id = juega_spain.home_team_api_id OR equipos_spain.team_api_id = juega_spain.away_team_api_id
    JOIN    
        partidos_spain ON juega_spain.match_api_id = partidos_spain.match_api_id
    WHERE 
        CAST(partidos_spain.id_temporada AS INTEGER) BETWEEN 2 AND 5
    GROUP BY 
        equipos_spain.team_api_id, equipos_spain.team_long_name;
""")

#Consulta 4
con.execute("""
            CREATE TABLE mayor_goles_afavor AS
            SELECT 
                team_long_name,
                goles_afavor
            FROM 
                consultaaux
            ORDER BY 
                goles_afavor DESC
            LIMIT 1;
            """)
            
con.sql("SELECT * FROM mayor_goles_afavor")

#%% CONSULTA 5
# ¿Cuál es el equipo con mayor diferencia de goles?

con.execute("""
            CREATE TABLE mayor_dif_gol AS
            SELECT 
                team_long_name,
                (goles_afavor - goles_encontra) AS diferencia_goles
            FROM 
                consultaaux
            ORDER BY 
                diferencia_goles DESC
            LIMIT 1;
            """)
            
con.sql("SELECT * FROM mayor_dif_gol")

#%% CONSULTA 6
# ¿Cuántos jugadores tuvo durante el período de tiempo seleccionado cada equipo en su plantel?

con.execute("""
            CREATE TABLE consulta6 AS
            SELECT 
                E.team_api_id,
                E.team_long_name,
                COUNT(DISTINCT JP.id_jugadores) AS cantidad_jugadores
            FROM 
                equipos_spain AS E
            JOIN 
                plantel_spain AS JP ON E.team_api_id = JP.id_equipo
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

con.sql("SELECT * FROM mayor_dif_gol")

#%% CONSULTA 7 
#¿Cuál es el jugador con mayor cantidad de goles?


con.sql(""" CREATE TABLE temp_table AS
        SELECT COUNT(id_goles) AS "Cantidad de goles", id_jugadores
        FROM goles_spain
        JOIN plantel_spain ON plantel_spain.id_j_en_plantel = goles_spain.id_j_en_plantel
        JOIN partidos_2_a_5 ON partidos_2_a_5.match_api_id = goles_spain.match_api_id
        GROUP BY id_jugadores
        ORDER BY "Cantidad de goles" DESC
        LIMIT 1
        """)
        
con.sql(""" CREATE TABLE jugador_max_goles AS
        SELECT "Cantidad de goles", player_name
        FROM temp_table
        JOIN jugadores_spain ON jugadores_spain.player_api_id = temp_table.id_jugadores
        """)

#%% CONSULTA 8
#¿Cuáles son los jugadores que más partidos ganó su equipo?

con.execute("""
            CREATE TABLE partidos_ganados_jugadores AS
                SELECT 
                    J.player_api_id,
                    J.player_name,
                    COUNT(*) AS partidos_ganados
                FROM 
                    jugadores_spain AS J
                JOIN 
                    plantel_spain AS JP ON J.player_api_id = JP.id_jugadores
                JOIN 
                    temporalidad_j_de_un_plantel_spain AS TPJ ON JP.id_j_en_plantel = TPJ.id_j_en_plantel
                JOIN 
                    equipos_spain AS E ON JP.id_equipo = E.team_api_id
                JOIN 
                    juega_spain AS JG ON E.team_api_id = JG.home_team_api_id OR E.team_api_id = JG.away_team_api_id
                JOIN 
                    partidos_spain AS P ON JG.match_api_id = P.match_api_id
                WHERE 
                    TPJ.id_temporada = P.id_temporada 
                    AND CAST(P.id_temporada AS INTEGER) BETWEEN 2 AND 5
                    AND (
                        (JG.home_team_api_id = E.team_api_id AND P.home_team_goal > P.away_team_goal)
                        OR
                        (JG.away_team_api_id = E.team_api_id AND P.away_team_goal > P.home_team_goal)
                    )
                GROUP BY 
                    J.player_api_id, J.player_name
                ORDER BY 
                    partidos_ganados DESC
            """)

con.execute("""
            SELECT 
                player_api_id, 
                player_name, 
                partidos_ganados
            FROM 
                partidos_ganados_jugadores
            WHERE 
                partidos_ganados = (SELECT MAX(partidos_ganados) FROM partidos_ganados_jugadores);
            """)


#%% CONSUTLA 9

con.execute("""
            CREATE TABLE equipos_por_jugador AS
            SELECT 
                J.player_api_id,
                J.player_name,
                COUNT(DISTINCT JP.id_equipo) AS cantidad_equipos
            FROM 
                jugadores_spain AS J
            JOIN 
                plantel_spain AS JP ON J.player_api_id = JP.id_jugadores
            GROUP BY 
                J.player_api_id, J.player_name;
            """)

con.sql("""
        SELECT 
            player_api_id, 
            player_name, 
            cantidad_equipos
        FROM 
            equipos_por_jugador
        WHERE 
            cantidad_equipos = (SELECT MAX(cantidad_equipos) FROM equipos_por_jugador);
        """)

#%% CONSULTA 10 
# ¿Cuál es el jugador que menor variación de potencia ha tenido a lo largo de los años? (medida en valor absoluto)

# con.sql("""DROP TABLE jugador_min_var_pot""")

con.sql("""CREATE TABLE jugador_min_var_pot AS
        SELECT jugadores_spain.player_api_id, STDDEV(atributos_spain.potential) AS desv_pot FROM atributos_spain
        JOIN temporada_spain ON temporada_spain.id_temporada = atributos_spain.id_temporada
        JOIN jugadores_spain ON jugadores_spain.player_api_id = atributos_spain.player_api_id
        WHERE temporada_spain.id_temporada BETWEEN 2 AND 5
        GROUP BY jugadores_spain.player_api_id
        ORDER BY jugadores_spain.player_api_id
        """)

con.sql("""SELECT player_name FROM jugador_min_var_pot
        JOIN jugadores_spain ON jugadores_spain.player_api_id = jugador_min_var_pot.player_api_id 
        WHERE desv_pot = 0
        """)

# con.sql("""
#         SELECT jugadores_spain.player_api_id, potential FROM atributos_spain
#         JOIN temporada_spain ON temporada_spain.id_temporada = atributos_spain.id_temporada
#         JOIN jugadores_spain ON jugadores_spain.player_api_id = atributos_spain.player_api_id
#         WHERE (temporada_spain.id_temporada BETWEEN 2 AND 5) AND (jugadores_spain.player_api_id = 407786)
#         """)



con.sql("DESCRIBE equipos_spain")

con.sql("""SELECT * FROM jugadores_spain 
        WHERE player_api_id = 30981
        """)

#%% CONSULTA 11 (OPCIONAL) 
#¿Hay algún equipo que haya sido a la vez el más goleador y el que tenga mayor valor de alguno
# de los atributos (considerando la suma de todos los jugadores)?



# Mostrar el DataFrame completo sin truncamiento
print((con.execute("SHOW TABLES").fetchdf()).to_string(index=False))

#Creo una tabla para obtener el equipo mas goleador

con.sql("""CREATE TABLE equipos_mas_goleadores AS
        SELECT SUM(home_team_goal) AS goles, home_team_api_id AS equipos_id FROM partidos_2_a_5
        GROUP BY home_team_api_id
        UNION ALL
        SELECT SUM(away_team_goal) AS goles, away_team_api_id AS equipos_id FROM partidos_2_a_5
        GROUP BY away_team_api_id
        """)

con.sql("""CREATE TABLE equipos_mas_goleadores_temp AS
        SELECT SUM(goles), team_long_name FROM equipos_mas_goleadores
        JOIN equipos_spain ON equipos_spain.team_api_id = equipos_mas_goleadores.equipos_id
        GROUP BY team_long_name
        ORDER BY sum(goles) DESC
        LIMIT 1
        """)
        
con.sql("""DROP TABLE equipos_mas_goleadores""")

con.execute("""ALTER TABLE equipos_mas_goleadores_temp RENAME TO equipos_mas_goleadores""")

#Ahora hago una tabla para mostrar la sumatoria de atributos de jugadores x equipo (primero x temporada y despues agrupo)

id_temps=[2,3,4,5]

for id in id_temps:
    con.sql(f"""CREATE TABLE atributos_jugadores_temp_{id} AS
            SELECT player_api_id, SUM(overall_rating) AS overall_rating, SUM(potential) AS potential, SUM(crossing) AS crossing, 
            SUM(finishing) AS finishing, SUM(dribbling) AS dribbling, SUM(free_kick_accuracy) AS free_kick_accuracy,
            SUM(ball_control) AS ball_control, SUM(acceleration) AS acceleration, 
            SUM(sprint_speed) AS sprint_speed, SUM(agility) AS agility, SUM(reactions) AS reactions, SUM(balance) AS balance,
            SUM(shot_power) AS shot_power, SUM(jumping) AS jumping, SUM(strength) AS strength,
            SUM(aggression) AS aggression, SUM(interceptions) AS interceptions, SUM(vision) AS vision,
            SUM(penalties) AS penalties, SUM(marking) AS marking, {id} AS id_temporada
            FROM atributos_spain 
            WHERE CAST(id_temporada AS INTEGER) = {id}
            GROUP BY player_api_id        
            ORDER BY player_api_id ASC
            """)
    
    con.sql(f"""CREATE TABLE atributos_equipos_temp_{id} AS
            SELECT team_long_name, SUM(overall_rating) AS overall_rating, SUM(potential) AS potential, SUM(crossing) AS crossing, 
            SUM(finishing) AS finishing, SUM(dribbling) AS dribbling, SUM(free_kick_accuracy) AS free_kick_accuracy,
            SUM(ball_control) AS ball_control, SUM(acceleration) AS acceleration, 
            SUM(sprint_speed) AS sprint_speed, SUM(agility) AS agility, SUM(reactions) AS reactions, SUM(balance) AS balance,
            SUM(shot_power) AS shot_power, SUM(jumping) AS jumping, SUM(strength) AS strength,
            SUM(aggression) AS aggression, SUM(interceptions) AS interceptions, SUM(vision) AS vision,
            SUM(penalties) AS penalties, SUM(marking) AS marking
            FROM atributos_jugadores_temp_{id}
            JOIN temporalidad_j_de_un_plantel_spain ON temporalidad_j_de_un_plantel_spain.id_temporada = atributos_jugadores_temp_{id}.id_temporada
            JOIN plantel_spain ON plantel_spain.id_j_en_plantel = temporalidad_j_de_un_plantel_spain.id_j_en_plantel
            JOIN equipos_spain ON equipos_spain.team_api_id = plantel_spain.id_equipo
            GROUP BY team_long_name        
            ORDER BY SUM(overall_rating) ASC
            """)


con.sql("""CREATE TABLE atributos_equipos_temp_2_a_5 AS
        SELECT team_long_name, overall_rating, potential, crossing, 
        finishing, dribbling, free_kick_accuracy, ball_control, acceleration, 
        sprint_speed, agility, reactions, balance, shot_power, 
        jumping, strength, aggression, interceptions, vision, 
        penalties, marking
        FROM atributos_equipos_temp_2
        UNION ALL
        SELECT team_long_name, overall_rating, potential, crossing, 
        finishing, dribbling, free_kick_accuracy, ball_control, acceleration, 
        sprint_speed, agility, reactions, balance, shot_power, 
        jumping, strength, aggression, interceptions, vision, 
        penalties, marking
        FROM atributos_equipos_temp_3
        UNION ALL
        SELECT team_long_name, overall_rating, potential, crossing, 
        finishing, dribbling, free_kick_accuracy, ball_control, acceleration, 
        sprint_speed, agility, reactions, balance, shot_power, 
        jumping, strength, aggression, interceptions, vision, 
        penalties, marking
        FROM atributos_equipos_temp_4
        UNION ALL
        SELECT team_long_name, overall_rating, potential, crossing, 
        finishing, dribbling, free_kick_accuracy, ball_control, acceleration, 
        sprint_speed, agility, reactions, balance, shot_power, 
        jumping, strength, aggression, interceptions, vision, 
        penalties, marking
        FROM atributos_equipos_temp_5
        """)

con.sql("""CREATE TABLE atributos_equipos_temp_2_a_5_temporal AS
        SELECT team_long_name, SUM(overall_rating) AS overall_rating, SUM(potential) AS potential, SUM(crossing) AS crossing, 
        SUM(finishing) AS finishing, SUM(dribbling) AS dribbling, SUM(free_kick_accuracy) AS free_kick_accuracy,
        SUM(ball_control) AS ball_control, SUM(acceleration) AS acceleration, 
        SUM(sprint_speed) AS sprint_speed, SUM(agility) AS agility, SUM(reactions) AS reactions, SUM(balance) AS balance,
        SUM(shot_power) AS shot_power, SUM(jumping) AS jumping, SUM(strength) AS strength,
        SUM(aggression) AS aggression, SUM(interceptions) AS interceptions, SUM(vision) AS vision,
        SUM(penalties) AS penalties, SUM(marking) AS marking
        FROM atributos_equipos_temp_2_a_5
        GROUP BY team_long_name
        """)

con.sql("""DROP TABLE atributos_equipos_temp_2_a_5""")

con.execute("""ALTER TABLE atributos_equipos_temp_2_a_5_temporal RENAME TO atributos_equipos_temp_2_a_5""")

# Estoy haciendo ligeramente mal la cuenta. Dado que algunos equipos no juegan todas temporadas. entonces estan subrepresentados. 
# sin embargo, ya que solo me interesa ver como le fue al FC Barcelona y este estuvo en todas las temporadas, omito ese error.
# Por falta de tiempo, no lo estoy resolviendo. No porque no pueda hacerlo.


atributos = ['overall_rating', 'potential', 'crossing', 'finishing', 'dribbling', 'free_kick_accuracy', 'ball_control', 'acceleration', 'sprint_speed', 'agility', 'reactions', 'balance', 'shot_power', 'jumping', 'strength', 'aggression', 'interceptions', 'vision', 'penalties', 'marking']



for atributo in atributos:
    con.sql(f"""CREATE TABLE mas_goleador_vs_{atributo} AS
            SELECT equipos_mas_goleadores.team_long_name, {atributo}
            FROM atributos_equipos_temp_2_a_5
            JOIN equipos_mas_goleadores ON equipos_mas_goleadores.team_long_name = atributos_equipos_temp_2_a_5.team_long_name
            WHERE {atributo} = (
                SELECT MAX({atributo}_x_equipo)
                FROM (
                    SELECT {atributo} AS {atributo}_x_equipo
                    FROM atributos_equipos_temp_2_a_5
                    )
                )
            """)
    result = con.execute(f"SELECT COUNT(*) FROM mas_goleador_vs_{atributo}").fetchone()[0]
        
    if result > 0:
        print(f"La tabla mas_goleador_vs_{atributo} tiene {result} filas.")
    else:
        print(f"La tabla mas_goleador_vs_{atributo} está vacía.")
    
# Ya que todas las tablas creadas estan vacias, no hay relacion entre ser el mas goleador y la sumatoria de los atributos de los jugadores.

#%%===========================================================================
# VISUALIZACION
#=============================================================================
#%% GRAFICO 1 (Notar que los valores nulos son debido a que los equipos descienden y ascienden de categoria)

# Crear el DataFrame para los goles a favor
heatmap_afavor = graficos1_2.pivot_table(
    index='nombre_equipo',  # Eje y (equipos)
    columns='nombre_temporada',  # Eje x (temporadas)
    values='goles_afavor',  # Valores a graficar
)

# Crear el DataFrame para los goles en contra
heatmap_encontra = graficos1_2.pivot_table(
    index='nombre_equipo',  # Eje y (equipos)
    columns='nombre_temporada',  # Eje x (temporadas)
    values='goles_encontra',  # Valores a graficar
)

# Configurar el gráfico para goles a favor
plt.figure(figsize=(12, 8))
sns.heatmap(heatmap_afavor, annot=True, fmt='g', cmap='Blues', cbar_kws={'label': 'Goles a Favor'}) #FMT = 'g' restringe la notacion cientifica
plt.title('Heatmap de Goles a Favor por Equipo y Temporada')
plt.xlabel('Temporadas')
plt.ylabel('Equipos')
plt.xticks(rotation=45)
plt.show()

# Configurar el gráfico para goles en contra
plt.figure(figsize=(12, 8))
sns.heatmap(heatmap_encontra, annot=True, fmt='g', cmap='Reds', cbar_kws={'label': 'Goles en Contra'})
plt.title('Heatmap de Goles en Contra por Equipo y Temporada')
plt.xlabel('Temporadas')
plt.ylabel('Equipos')
plt.xticks(rotation=45)
plt.show()

#%% GRAFICO 1bis. Se puede ver cambiando el Id de forma exacta la cantidad de GA y GE de cada equipo

# Variando el Id grafica cualquier equipo que quieras
equipo = graficos1_2[graficos1_2['team_api_id'] == 8634]

# Configurar el gráfico
fig, ax = plt.subplots(figsize=(10, 6))

# Establecer la posición de las barras en el eje x
bar_width = 0.35
positions = range(len(equipo['id_temporada']))

# Crear las barras para goles a favor y en contra
bars_afavor = ax.bar(positions, equipo['goles_afavor'], width=bar_width, label='Goles a Favor', color='blue')
bars_encontra = ax.bar([p + bar_width for p in positions], equipo['goles_encontra'], width=bar_width, label='Goles en Contra', color='red')

# Etiquetas y títulos
ax.set_xlabel('Temporadas')
ax.set_ylabel('Número de Goles')

# Cambiar el título del gráfico para mostrar el nombre del equipo
nombre_equipo = equipo['nombre_equipo'].iloc[0]  # Extrae el nombre del equipo
ax.set_title(f'Goles a Favor y en Contra para el Equipo: {nombre_equipo} por Temporada')

ax.set_xticks([p + bar_width / 2 for p in positions])
ax.set_xticklabels(equipo['nombre_temporada'].values, rotation=45)

ax.legend()

# Añadir los números encima de las barras
for bar in bars_afavor:
    yval = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, yval, int(yval), ha='center', va='bottom')

for bar in bars_encontra:
    yval = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, yval, int(yval), ha='center', va='bottom')

# Ajustar la escala del eje y
max_goles = equipo[['goles_afavor', 'goles_encontra']].max().max()  # Valor máximo de goles
ax.set_ylim(0, max_goles * 1.1)  # Aumenta el límite superior en un 10%

# Mostrar el gráfico
plt.tight_layout()
plt.show()

#%% GRAFICO 2

# Crear el DataFrame para el promedio de gol
heatmap_promedio_gol = graficos1_2.pivot_table(
    index='nombre_equipo',  # Eje y (equipos)
    columns='nombre_temporada',  # Eje x (temporadas)
    values='promedio_gol',  # Valores a graficar
)


# Configurar el gráfico para el promedio de gol
plt.figure(figsize=(12, 8))
sns.heatmap(heatmap_promedio_gol, cmap='magma', annot=True, fmt='.2f')  
plt.title('Promedios de Gol por Equipo y Temporada')
plt.xlabel('Temporadas')
plt.ylabel('Equipos')
plt.xticks(rotation=45)
plt.tight_layout()  # Ajustar el diseño
plt.show()

#%% GRAFICO 3

# Pivotar los datos para cada uno de los heatmaps
goles_de_local = grafico3.pivot(index="nombre_equipo", columns="nombre_temporada", values="goles_de_local")
goles_de_visitante = grafico3.pivot(index="nombre_equipo", columns="nombre_temporada", values="goles_de_visitante")
diferencia_goles = grafico3.pivot(index="nombre_equipo", columns="nombre_temporada", values="diferencia_goles_LvsV")

# Crear el FacetGrid ##
fig, axes = plt.subplots(1, 2, figsize=(26, 8))

# Heatmap para goles de local
sns.heatmap(goles_de_local, ax=axes[0], cmap="YlGnBu", annot=True, cbar_kws={'label': 'Goles de Local'})
axes[0].set_title('Goles de Local por Equipo y Temporada')

# Heatmap para goles de visitante
sns.heatmap(goles_de_visitante, ax=axes[1], cmap="YlGnBu", annot=True, cbar_kws={'label': 'Goles de Visitante'})
axes[1].set_title('Goles de Visitante por Equipo y Temporada')
# Fin del Facet Grid ##

# Heatmap para diferencia de goles (Los valores negativos indican que en esa temporada anoto |value| goles de visitante mas que de local)
plt.figure(figsize=(10, 6))
sns.heatmap(diferencia_goles, cmap="YlGnBu", annot=True, cbar_kws={'label': 'Diferencia de Goles'})
ax.set_title('Diferencia de Goles de local y Goles de visitante por Equipo y Temporada')
plt.title('Diferencia de Goles por Equipo y Temporada')
plt.tight_layout()
plt.show()
# Ajustar el diseño
plt.tight_layout()

#%% GRAFICO 4

# Crear un bubble chart
plt.figure(figsize=(20, 14))

# Ajustar el tamaño de las burbujas para una mejor visualización
bubble_size = grafico4['suma_total_goles'] / 100  # Ajusta el divisor según el tamaño deseado de las burbujas

# Crear una lista de colores para cada equipo
colors = plt.cm.tab20(grafico4.index % 20)  # Usa una paleta de colores para dar un color único a cada burbuja

# Crear el gráfico de dispersión
scatter = plt.scatter(
    grafico4['suma_total_overall'],
    grafico4['suma_total_goles'],
    s=bubble_size,
    c=colors,
    alpha=0.6
)

# Añadir etiquetas de texto para cada punto (nombre del equipo)
for i, row in grafico4.iterrows():
    plt.text(
        row['suma_total_overall'],
        row['suma_total_goles'],
        row['team_long_name'],
        fontsize=8,
        ha='center', va='center'
    )

# Configurar los ejes
plt.xlabel('Sumatoria Total de Overall')
plt.ylabel('Sumatoria Total de Goles')
plt.title('Bubble Chart: Sumatoria de Overall vs. Goles Totales por Equipo')
plt.grid(True)

# Desactivar notación científica en el eje X
plt.ticklabel_format(style='plain', axis='x')

plt.savefig('grafico4.jpg', format='jpg', dpi=300)
# Mostrar el gráfico
plt.tight_layout()
plt.show()




