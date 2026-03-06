from mongo_client import get_db
from log_parser import parse_apache, parse_app_json, parse_syslog

db = get_db()
collection = db["logs_parsed"]


def ingest_apache(file):

    with open(file) as f:

        for line in f:

            parsed = parse_apache(line)

            if parsed:
                collection.insert_one(parsed)


def ingest_json(file):

    with open(file) as f:

        for line in f:

            parsed = parse_app_json(line)

            collection.insert_one(parsed)


def ingest_syslog(file):

    with open(file) as f:

        for line in f:

            parsed = parse_syslog(line)

            collection.insert_one(parsed)


if __name__ == "__main__":

    ingest_apache("data/apache.log")
    ingest_json("data/app.json")
    ingest_syslog("data/syslog.log")

    print("Logs successfully inserted into MongoDB")