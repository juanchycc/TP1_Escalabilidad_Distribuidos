import argparse
import os

FILENAME = "docker-compose-dev.yaml"

parser = argparse.ArgumentParser()
parser.add_argument("-q1", type=int, help="amount of nodes in the first query")
parser.add_argument("-q3", type=int, help="amount of nodes in the third query")
parser.add_argument("-avg", type=int, help="amount of nodes in the avg query")
args = parser.parse_args()


# list of names of output files
output_files = ["out_file_q1.csv", "out_file_q3.csv"]

# create output files
for o in output_files:
    file_path = "./file_writer/" + o
    if os.path.exists(file_path):
        os.remove(file_path)
    file = open(file_path, "w")
    file.close()

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

post_handler_text = """  post_handler:
    container_name: post_handler
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
    volumes:
      - ./post_handler/config.ini:/config.ini
    ports:
      - 12345:12345

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
    volumes:
      - ./flights_filter/config.ini:/config.ini

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
    volumes:
      - ./flights_avg/config.ini:/config.ini

"""

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

with open(FILENAME, 'w') as f:
    f.write(initial_text + rabbit_text + post_handler_text +
            final_text_plus_3 + final_text_max + file_writer_text + final_text_avg)
