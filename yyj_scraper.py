from dotenv import dotenv_values
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

# from urllib.parse import quote_plus

config = dotenv_values(".env")


def get_client():
    """
    :return: a connection to the database
    """
    uri = "mongodb://%s" % (
        # quote_plus(config["DB_USER"]),
        # quote_plus(config["DB_PWORD"]),
        config["DB_HOST"],
    )
    try:
        client = MongoClient(uri)
    except Exception as e:
        print("Error connecting to MongoDB Platform: ", e)
        exit(1)
    return client


def get_flights(url, table_id):
    """
    Gets the table of flights from the given url
    :param url: the url to scrape
    :param table_id: the id of the table to scrape
    :return: the table of flights
    """
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    # get the table with id "flightsToday", select rows with class "arrival" or "departure"
    table = soup.find("table", {"id": table_id})
    if table:
        table = table.find_all("tr", class_=["arrival", "departure"])

    return table


def parse_flights(table):
    """
    Parses the table of flights
    :caveat: this function breaks on every Jan 1st for delayed flights
    :param table: the table of flights
    :return: list flights
    """
    flights = []
    for row in table:
        flight = {}
        date = row.find("small").text.strip()
        scheduled_time = row.find("div").text.strip()
        # only delayed flight have this div
        actual_time_div = row.find("div", class_="bubble")
        if actual_time_div:
            actual_time = actual_time_div.find_all("div")[1].text.strip()
        else:
            actual_time = None
        flight["airline"] = row.find("span").text.strip()
        flight["flight_num"] = row.find_all("td")[1].text.strip()
        flight["src_dest"] = row.find_all("td")[2].text.strip()
        flight["gate"] = row.find("td", class_="ft-gate").text.strip()

        # timestamps in UTC because that's what MongoDB uses
        flight["scheduled_timestamp"] = (
            (datetime.strptime(date + " " + scheduled_time, "%a %b %d %I:%M %p"))
            .replace(year=datetime.now().year)
            .astimezone(timezone.utc)
        )
        if actual_time:
            flight["actual_timestamp"] = (
                (datetime.strptime(date + " " + actual_time, "%a %b %d %I:%M %p"))
                .replace(year=datetime.now().year)
                .astimezone(timezone.utc)
            )

        # one of the advantages of MongoDB is flexibile schema
        # i may want to take advantage of that by only storing keys with not null values
        if "departure" in row["class"]:
            flight["type"] = "departure"
        else:
            flight["type"] = "arrival"

        flights.append(flight)

    return flights


def main():
    delayed_flights = get_flights(config["URL"], "flightsYesterday")
    flight_table = get_flights(config["URL"], "flightsToday")
    flights = parse_flights(flight_table)
    client = get_client()
    db = client[config["DB_NAME"]]
    flights_collection = db[config["COLLECTION"]]

    update_str = ""
    if delayed_flights:
        delayed_flights = parse_flights(delayed_flights)
        # for each entry in delayed flights find the corresponding flight
        # in the flights collection by scheduled_timestamp and flight_num
        # and update the actual_timestamp
        updated = 0
        for delayed_flight in delayed_flights:
            query = {
                "scheduled_timestamp": delayed_flight["scheduled_timestamp"],
                "flight_num": delayed_flight["flight_num"],
            }
            update = {"$set": {"actual_timestamp": delayed_flight["actual_timestamp"]}}
            try:
                result = flights_collection.update_one(query, update)
                updated += result.modified_count
            except Exception as e:
                print("Error updating document in MongoDB: ", e)
                exit(1)
        update_str = f"Updated {updated} documents\n"

    try:
        results = flights_collection.insert_many(flights)
    except Exception as e:
        print("Error inserting documents into MongoDB: ", e)
        exit(1)

    # write to log file with timestamp
    with open("flights.log", "a") as f:
        f.write(
            f"""
              {datetime.now().strftime('%Y-%m-%d %H:%M')}: 
              Inserted {len(results.inserted_ids)} documents 
              into {config['COLLECTION']}\n
              """
            + update_str
        )
    client.close()


if __name__ == "__main__":
    main()
