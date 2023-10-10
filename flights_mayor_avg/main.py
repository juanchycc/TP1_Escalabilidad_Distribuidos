import os
import logging
from configparser import ConfigParser
from common.filter import FilterMayorAvg
from common.middleware import Middleware
from common.protocol import Serializer


def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["in_avg_exchange"] = os.getenv(
            'IN_AVG_EXCHANGE', config["DEFAULT"]["IN_AVG_EXCHANGE"])
        config_params["in_flights_exchange"] = os.getenv(
            'IN_FLIGHTS_EXCHANGE', config["DEFAULT"]["IN_FLIGHTS_EXCHANGE"])
        config_params["out_exchange"] = os.getenv(
            'OUT_EXCHANGE', config["DEFAULT"]["OUT_EXCHANGE"])
        config_params["fields"] = os.getenv(
            'FIELDS', config["DEFAULT"]["FIELDS"])
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    middleware = Middleware(config_params["in_avg_exchange"], config_params["in_flights_exchange"],
                            config_params["out_exchange"])

    fields = config_params["fields"].split(',')
    num_filters = int(os.environ.get('FLIGHTS_MAYOR_AVG_AMOUNT', 1))
    num_groups = int(os.environ.get('FLIGHTS_AVG_JOURNEY_AMOUNT', 1))
    serializer = Serializer(middleware, fields, num_filters, num_groups)

    filter = FilterMayorAvg(
        serializer)
    filter.run()


if __name__ == "__main__":
    main()
