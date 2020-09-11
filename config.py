from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")

# curl -X GET "http://127.0.0.1:8000/items/6?q=hello_world"