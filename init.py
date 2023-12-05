import argparse

FILENAME = "docker-compose-dev.yaml"
CONFIG_CLIENT_PATH = "client/config.ini"

parser = argparse.ArgumentParser()
parser.add_argument("-q1", type=int, help="amount of nodes in the first query")
parser.add_argument("-q3", type=int, help="amount of nodes in the third query")
parser.add_argument("-avg", type=int, help="amount of nodes in the avg query")
# cantidad de nodos en mayor avg filter
parser.add_argument("-Mavg", type=int, help="amount of nodes in the avg query")
parser.add_argument("-q4", type=int, help="amount of nodes in the avg query")
parser.add_argument(
    "-q2", type=int, help="amount of nodes in the second query")
parser.add_argument(
    "-m", type=int, help="amount of managers")
parser.add_argument(
    "-a", type=int, help="amount of airports handlers")
parser.add_argument(
    "-ja", type=int, help="amount of join avgs")
parser.add_argument(
    "-p", type=int, help="amount of post handlers")
args = parser.parse_args()

layers = {"flights_filter_": args.q1,
          "flights_max_": args.q3, "flights_avg_": args.avg, "flights_mayor_avg_": args.Mavg, "flights_avg_by_journey_": args.q4, "flights_filter_distance_": args.q2, "manager_": args.m, "airport_fligths_handler_": args.a, "post_handler:": args.p}

initial_text = """version: '3.9'
services:
"""

rabbit_text = """  rabbitmq:
    build:
      context: ./rabbitmq
    ports:
      - 15672:15672
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:15672" ]
      interval: 10s
      timeout: 5s
      retries: 10

"""

post_handler_text = """  post_handler_#:
    container_name: post_handler_#
    build:
      context: ./post_handler
      dockerfile: Dockerfile
    image: post_handler:latest
    entrypoint: python3 ./main.py
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - FLIGHTS_FILTER_PLUS_AMOUNT=&
      - AIRPORTS_HANDLER_AMOUNT=$
      - FLIGHTS_FILTER_AVG_AMOUNT=!
      - POST_HANDLER_ID=#
    volumes:
      - ./post_handler/config.ini:/config.ini
      - ./utils:/utils
      - ./middleware:/middleware
    ports:
      - 1235#:1235#
"""

flights_filter_plus_3_text = """  flights_filter_plus_3_#:
    container_name: flights_filter_#
    build:
      context: ./flights_filter
      dockerfile: Dockerfile
    image: flights_filter:latest
    entrypoint: python3 ./main.py
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - FLIGHTS_FILTER_PLUS_AMOUNT=& 
      - FLIGHTS_MAX_AMOUNT=$
      - FLIGHT_FILTER_ID=#
    volumes:
      - ./flights_filter/config.ini:/config.ini
      - ./utils:/utils
      - ./middleware:/middleware
"""

flights_filter_max_text = """  flights_filter_max_#:
    container_name: flights_max_#
    build:
      context: ./flights_max
      dockerfile: Dockerfile
    image: flights_max:latest
    entrypoint: python3 ./main.py
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - FLIGHTS_MAX_ID=#
      - FLIGHTS_MAX_AMOUNT=$
    volumes:
      - ./flights_max/config.ini:/config.ini
      - ./utils:/utils
      - ./middleware:/middleware
"""

file_writer_text = """  file_writer:
    container_name: file_writer
    build:
      context: ./file_writer
      dockerfile: Dockerfile
    image: file_writer:latest
    entrypoint: python3 ./main.py
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
    volumes:
      - ./file_writer/config.ini:/config.ini
      - ./file_writer/:/out_files/
      - ./utils:/utils
"""

flights_filter_avg = """  flights_filter_avg_#:
    container_name: flights_avg_#
    build:
      context: ./flights_avg
      dockerfile: Dockerfile
    image: flights_avg:latest
    entrypoint: python3 ./main.py
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - FLIGHTS_FILTER_AVG_AMOUNT=$
      - ID=#
      - JOIN_AVG_AMOUNT=&
      - FLIGHTS_MAYOR_AVG_AMOUNT=%
    volumes:
      - ./flights_avg/config.ini:/config.ini
      - ./utils:/utils
      - ./middleware:/middleware
"""

flights_filter_distance_text = """  flights_filter_distance_#:
    container_name: flights_filter_distance_#
    build:
      context: ./flights_filter_distance
      dockerfile: Dockerfile
    image: flights_filter_distance:latest
    entrypoint: python3 ./main.py
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - FLIGHTS_FILTER_DISTANCE_ID=#
      - FLIGHTS_FILTER_DISTANCE_AMOUNT=$
    volumes:
      - ./flights_filter_distance/config.ini:/config.ini
      - ./utils:/utils
      - ./middleware:/middleware
"""

flights_filter_distance_text = flights_filter_distance_text.replace(
    '$', str(args.q2))
final_text_distance = ""
for i in range(1, args.q2 + 1):
    final_text_distance = final_text_distance + \
        flights_filter_distance_text.replace('#', str(i))

flights_filter_avg = flights_filter_avg.replace('$', str(args.avg))
flights_filter_avg = flights_filter_avg.replace('&', str(args.ja))
flights_filter_avg = flights_filter_avg.replace('%', str(args.Mavg))

flights_filter_avg_final = ""
for i in range(1, args.avg + 1):
    flights_filter_avg_final += flights_filter_avg.replace('#', str(i))


flights_filter_plus_3_text = flights_filter_plus_3_text.replace(
    '$', str(args.q3))
final_text_plus_3 = ""
for i in range(1, args.q1 + 1):
    final_text_plus_3 = final_text_plus_3 + \
        flights_filter_plus_3_text.replace('#', str(i))

final_text_plus_3 = final_text_plus_3.replace('&', str(args.q1))
final_text_plus_3 = final_text_plus_3.replace(
    '$', str(args.q3))

final_text_max = ""
for i in range(1, args.q3 + 1):
    final_text_max = final_text_max + \
        flights_filter_max_text.replace('#', str(i))

final_text_max = final_text_max.replace('$', str(args.q3))

final_text_avg = ""
for i in range(1, args.avg + 1):
    final_text_avg = final_text_avg + \
        flights_filter_avg.replace('#', str(i))

flights_join_avg = """  flights_join_avg_#:
    container_name: flights_join_avg_#
    build:
      context: ./join_avg
      dockerfile: Dockerfile
    image: join_avg:latest
    entrypoint: python3 ./main.py
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - ID=#
    volumes:
      - ./join_avg/config.ini:/config.ini
      - ./utils:/utils
      - ./middleware:/middleware
"""

flights_join_avg_final = ""
for i in range(1, args.ja + 1):
    flights_join_avg_final += flights_join_avg.replace('#', str(i))


flights_mayor_avg = """  flights_mayor_avg_#:
    container_name: flights_mayor_avg_#
    build:
      context: ./flights_mayor_avg
      dockerfile: Dockerfile
    image: flights_mayor_avg:latest
    entrypoint: python3 ./main.py
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - FLIGHTS_MAYOR_AVG_ID=#
      - FLIGHTS_MAYOR_AVG_AMOUNT=$
      - FLIGHTS_AVG_JOURNEY_AMOUNT=&
    volumes:
      - ./flights_mayor_avg/config.ini:/config.ini
      - ./utils:/utils
      - ./middleware:/middleware
"""

final_text_mayor_avg = ""
for i in range(1, args.Mavg + 1):
    final_text_mayor_avg = final_text_mayor_avg + \
        flights_mayor_avg.replace('#', str(i))

final_text_mayor_avg = final_text_mayor_avg.replace('$', str(args.Mavg))
final_text_mayor_avg = final_text_mayor_avg.replace('&', str(args.q4))

flights_avg_by_journey = """  flights_avg_by_journey_#:
    container_name: flights_avg_by_journey_#
    build:
      context: ./flights_avg_by_journey
      dockerfile: Dockerfile
    image: flights_avg_by_journey:latest
    entrypoint: python3 ./main.py
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - FLIGHTS_AVG_JOURNEY_ID=#
      - FLIGHTS_AVG_JOURNEY_AMOUNT=&
    volumes:
      - ./flights_avg_by_journey/config.ini:/config.ini
      - ./utils:/utils
      - ./middleware:/middleware
"""

manager = """  manager_#:
    container_name: manager_#
    build:
      context: ./manager
      dockerfile: Dockerfile
    image: manager:latest
    entrypoint: python3 ./main.py
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - MANAGER_ID=#
      - MANAGER_AMOUNT=$
      - LAYER=!
    volumes:
      - ./utils:/utils
      - /var/run/docker.sock:/var/run/docker.sock

"""

manager = manager.replace('!', str(layers).replace(' ', ''))
manager = manager.replace('$', str(args.m))

final_manager = ""
for i in range(1, args.m + 1):
    final_manager = final_manager + \
        manager.replace('#', str(i))

airport_fligths_handler_text = """  airport_fligths_handler_#:
    container_name: airport_fligths_handler_#
    build:
      context: ./airport_fligths_handler
      dockerfile: Dockerfile
    image: airport_fligths_handler:latest
    entrypoint: python3 ./main.py
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - HANDLER_ID=#
      - HANDLER_AMOUNT=$
      - FILTER_DISTANCE_AMOUNT=&
    volumes:
      - ./utils:/utils
      - ./middleware:/middleware

"""
airport_fligths_handler_text = airport_fligths_handler_text.replace(
    '$', str(args.a))
airport_fligths_handler_text = airport_fligths_handler_text.replace(
    '&', str(args.q2))
final_airport_fligths_handler = ""
for i in range(1, args.a + 1):
    final_airport_fligths_handler = final_airport_fligths_handler + \
        airport_fligths_handler_text.replace('#', str(i))

final_text_flights_avg_by_journey = ""
for i in range(1, args.q4 + 1):
    final_text_flights_avg_by_journey = final_text_flights_avg_by_journey + \
        flights_avg_by_journey.replace('#', str(i))
final_text_flights_avg_by_journey = final_text_flights_avg_by_journey.replace(
    '&', str(args.q4))

post_handler_text = post_handler_text.replace('&', str(args.q1))
post_handler_text = post_handler_text.replace('$', str(args.a))
post_handler_text = post_handler_text.replace('!', str(args.avg))

final_post_handler_text = ""
for i in range(1, args.p + 1):
    final_post_handler_text = final_post_handler_text + \
        post_handler_text.replace('#', str(i))

# Create docker-compose file
with open(FILENAME, 'w') as f:
    f.write(initial_text + rabbit_text + final_post_handler_text +
            final_text_plus_3 + final_text_max + file_writer_text + final_text_avg + flights_join_avg_final + final_text_mayor_avg + final_airport_fligths_handler + final_text_distance + final_text_flights_avg_by_journey + final_manager)

client_config_text = """[DEFAULT]
SERVER_IP = localhost
SERVER_PORT_BASE = 1235
LISTENER_PORT = 12340
LOGGING_LEVEL = INFO
BATCH_SIZE = 8192
FLIGHTS_FILENAME = flights_$.csv
AIRPORTS_FILENAME = airports_$.csv
POST_HANDLERS_AMOUNT = #
BASE_OUTPUT_PATH = ./out_file_q
QUERY_AMOUNT = 4
CLIENT_PORT_BASE = 1255
"""
client_config_text = client_config_text.replace('#', str(args.p))

# Create config.ini Client file
with open(CONFIG_CLIENT_PATH, 'w') as f:
    f.write(client_config_text)
