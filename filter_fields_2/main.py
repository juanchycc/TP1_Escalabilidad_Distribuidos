from filter import FilterDistance
import os
import logging


def initialize_config():
    
    config_params = {}
    config_params["logging_level"] = os.getenv("LOGGING_LEVEL","INFO")
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
    
    
    filter = FilterDistance()
    filter.run()   
    
    
    
if __name__ == "__main__":
    main()
    
