from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# ---------------------------------- CRUD API: ----------------------------------

def create_tables():
    queries = ["CREATE TABLE Owner(Owner_ID INTEGER , Owner_name TEXT, PRIMARY KEY(Owner_ID), CHECK(Owner_ID > 0));",
               "CREATE TABLE Apartment(ID INTEGER, Address TEXT, City TEXT, Country TEXT, Size INTEGER, UNIQUE(City, Address), PRIMARY KEY(ID), CHECK(ID > 0));",
               "CREATE TABLE Customer(Customer_ID INTEGER, Customer_name TEXT, PRIMARY KEY(Customer_ID), CHECK(Customer_ID > 0));",
               "CREATE TABLE Owns(Owner_ID INTEGER, ID INTEGER, PRIMARY KEY(ID) ,FOREIGN KEY(ID) REFERENCES Apartment(ID) ON DELETE CASCADE, FOREIGN KEY(Owner_ID) REFERENCES Owner ON DELETE CASCADE);",
               "CREATE TABLE Reserved(Customer_ID INTEGER, ID INTEGER, start_date DATE, end_date DATE, total_price FLOAT, FOREIGN KEY(ID) REFERENCES Apartment ON DELETE CASCADE, FOREIGN KEY(Customer_ID) REFERENCES Customer ON DELETE CASCADE);",
               "CREATE TABLE Reviewed(ID INTEGER, Customer_ID INTEGER, review_date DATE, rating INTEGER, review_text TEXT, PRIMARY KEY(ID, Customer_ID), FOREIGN KEY(ID) REFERENCES Apartment ON DELETE CASCADE, FOREIGN KEY(Customer_ID) REFERENCES Customer ON DELETE CASCADE);",
               "CREATE VIEW Apartment_Rating AS (SELECT ID, AVG(rating) AS average_rating FROM Reviewed GROUP BY ID);",
               "CREATE VIEW Customer_reservations AS SELECT C.Customer_id, C.Customer_name, COUNT(R.start_date) AS num_reservations FROM Customer C LEFT OUTER JOIN Reserved R ON C.Customer_id = R.Customer_id GROUP BY C.Customer_id, C.Customer_name;"
               "CREATE VIEW Owner_cities_count AS (SELECT O.Owner_id, O.Owner_name, COUNT(DISTINCT A.City) AS num_cities FROM (Owner O LEFT OUTER JOIN Owns OW ON O.Owner_id = OW.Owner_id) LEFT OUTER JOIN Apartment A ON OW.id = A.id GROUP BY O.Owner_id, O.Owner_name);",
               "CREATE VIEW Months AS SELECT 1 AS Month UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12;"]
# "CREATE VIEW Apartments_profit_in_year_and_month AS SELECT R.end_date.year AS Year, R.end_date.month AS Month, SUM(R.total_price) * 0.15 AS profit FROM Reserved R GROUP BY Year, Month;"
    conn = None
    try:
        conn = Connector.DBConnector()
        for query in queries:
            conn.execute(query)

    except Exception as e:
        print(e)
    finally:
        conn.close()


def clear_tables():
    queries = ["DELETE FROM Owner;",
               "DELETE FROM Apartment;",
               "DELETE FROM Customer;",
               "DELETE FROM Owns;",
               "DELETE FROM Reserved;"]
    queries.reverse()

    conn = None
    try:
        conn = Connector.DBConnector()
        for query in queries:
            conn.execute(query)

    except Exception as e:
        print(e)
    finally:
        conn.close()


def drop_tables():
    queries = ["DROP TABLE Owner;",
               "DROP TABLE Apartment;",
               "DROP TABLE Customer;",
               "DROP TABLE Owns;",
               "DROP TABLE Reserved;",
               "DROP TABLE Reviewed;",
               "DROP VIEW Apartment_rating;",
               "DROP VIEW Customer_reservations;"
               "DROP VIEW Owner_cities_count;",
               "DROP VIEW Months;"]
             #  "DROP VIEW Apartments_profit_in_year_and_month;"]
    queries.reverse()

    conn = None
    try:
        conn = Connector.DBConnector()
        for query in queries:
            conn.execute(query)

    except Exception as e:
        print(e)
    finally:
        conn.close()


def add_owner(owner: Owner) -> ReturnValue:
    # checks
    if owner.get_owner_name() is None or owner.get_owner_id() is None:
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Owner VALUES({id}, {name})").format(id=sql.Literal(owner.get_owner_id()),
                                                                         name=sql.Literal(owner.get_owner_name()))
        rows_affected, _ = conn.execute(query)
        if rows_affected != 1:
            return ReturnValue.ERROR

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def get_owner(owner_id: int) -> Owner:
    owner = Owner.bad_owner()

    if owner_id <= 0 or owner_id is None:
        return owner

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Owner WHERE Owner_ID = {id}").format(id=sql.Literal(owner_id))
        _, result = conn.execute(query)
        if result.size() == 1:
            owner = Owner(**result[0])
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return owner


def delete_owner(owner_id: int) -> ReturnValue:
    if owner_id <= 0 or owner_id is None:
        return ReturnValue.BAD_PARAMS
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Owner WHERE Owner_ID = {id}").format(id=sql.Literal(owner_id))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def add_apartment(apartment: Apartment) -> ReturnValue:
    # checks
    if None in [apartment.get_id(), apartment.get_address(),
                apartment.get_city(), apartment.get_country(), apartment.get_size()]:
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Apartment VALUES({id}, {address}, {city}, {country}, {size})").format(
            id=sql.Literal(apartment.get_id()),
            address=sql.Literal(apartment.get_address()),
            city=sql.Literal(apartment.get_city()),
            country=sql.Literal(apartment.get_country()),
            size=sql.Literal(apartment.get_size()))
        rows_affected, _ = conn.execute(query)
        if rows_affected != 1:
            return ReturnValue.ERROR

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def get_apartment(apartment_id: int) -> Apartment:
    apartment = Apartment.bad_apartment()

    if apartment_id <= 0 or apartment_id is None:
        return apartment

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Apartment WHERE ID = {id}").format(id=sql.Literal(apartment_id))
        _, result = conn.execute(query)
        if result.size() == 1:
            apartment = Apartment(**result[0])

    except Exception as e:
        print(e)
    finally:
        conn.close()
    return apartment


def delete_apartment(apartment_id: int) -> ReturnValue:
    if apartment_id <= 0 or apartment_id is None:
        return ReturnValue.BAD_PARAMS
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Apartment WHERE ID = {id}").format(id=sql.Literal(apartment_id))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def add_customer(customer: Customer) -> ReturnValue:
    # checks
    if customer.get_customer_name() is None or customer.get_customer_id() is None:
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Customer VALUES({id}, {name})").format(id=sql.Literal(customer.get_customer_id()),
                                                                            name=sql.Literal(
                                                                                customer.get_customer_name()))
        rows_affected, _ = conn.execute(query)
        if rows_affected != 1:
            return ReturnValue.ERROR

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def get_customer(customer_id: int) -> Customer:
    customer = Customer.bad_customer()

    if customer_id <= 0 or customer_id is None:
        return customer

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Customer WHERE Customer_ID = {id}").format(id=sql.Literal(customer_id))
        _, result = conn.execute(query)

        if result.size() == 1:
            customer = Customer(**result[0])

    except Exception as e:
        print(e)
    finally:
        conn.close()
    return customer


def delete_customer(customer_id: int) -> ReturnValue:
    if customer_id <= 0 or customer_id is None:
        return ReturnValue.BAD_PARAMS
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Customer WHERE Customer_ID = {id}").format(id=sql.Literal(customer_id))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
    if customer_id <= 0 or customer_id is None or apartment_id <= 0 or apartment_id is None or total_price <= 0 \
            or start_date is None or end_date is None or total_price is None:
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("INSERT INTO Reserved " +
                        "SELECT {customer_id}, {apartment_id}, {start_date}, {end_date}, {total_price} " +
                        "WHERE NOT EXISTS (SELECT 1 FROM Reserved AS R " +
                        "WHERE R.id = {apartment_id} AND (" +
                        "(R.start_date BETWEEN {start_date} AND {end_date}) " +
                        "OR (R.end_date BETWEEN {start_date} AND {end_date}) " +
                        "OR (R.start_date < {start_date} AND R.end_date > {end_date})));").format(
            customer_id=sql.Literal(customer_id),
            apartment_id=sql.Literal(apartment_id),
            start_date=sql.Literal(start_date.strftime('%Y-%m-%d')),
            end_date=sql.Literal(end_date.strftime('%Y-%m-%d')),
            total_price=sql.Literal(total_price))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.BAD_PARAMS

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR

    finally:
        conn.close()
    return ReturnValue.OK


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    if customer_id <= 0 or customer_id is None or apartment_id <= 0 or apartment_id is None or start_date is None:
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Reserved WHERE " +
                        "Customer_ID = {customer_id} AND ID = {apartment_id} AND start_date = {start_date}").format(
                            customer_id=(sql.Literal(customer_id)),
                            apartment_id=(sql.Literal(apartment_id)),
                            start_date=sql.Literal(start_date.strftime('%Y-%m-%d')))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    if (customer_id <= 0 or apartment_id <= 0 or customer_id is None or apartment_id is None
            or review_date is None or review_text is None or rating < 1 or rating > 10 or rating is None):
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("INSERT INTO Reviewed " +
                        "SELECT {apartment_id}, {customer_id}, {review_date}, {rating}, {review_text} " +
                        "WHERE EXISTS (SELECT 1 FROM Reserved AS R " +
                        "WHERE R.id = {apartment_id} AND R.Customer_ID = {customer_id} " +
                        "AND R.end_date < {review_date})").format(
                            customer_id=sql.Literal(customer_id),
                            apartment_id=sql.Literal(apartment_id),
                            review_date=sql.Literal(review_date.strftime('%Y-%m-%d')),
                            rating=sql.Literal(rating),
                            review_text=sql.Literal(review_text))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            # where the reviewer didn't reserve the apartment on time
            return ReturnValue.NOT_EXISTS

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # where customer or apartment not exist
        print(e)
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        # where customer already reviewed the apartment
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR

    finally:
        conn.close()
    return ReturnValue.OK


def customer_updated_review(customer_id: int, apartment_id: int, update_date: date,
                            new_rating: int, new_text: str) -> ReturnValue:
    if (customer_id <= 0 or apartment_id <= 0 or customer_id is None or apartment_id is None
            or update_date is None or new_text is None or new_rating < 1 or new_rating > 10 or new_rating is None):
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("UPDATE Reviewed " +
                        "SET review_date = {update_date}, rating = {new_rating}, review_text = {new_text} " +
                        "WHERE id = {apartment_id} AND Customer_ID = {customer_id} " +
                        "AND review_date < {update_date}").format(
                            customer_id=sql.Literal(customer_id),
                            apartment_id=sql.Literal(apartment_id),
                            update_date=sql.Literal(update_date.strftime('%Y-%m-%d')),
                            new_rating=sql.Literal(new_rating),
                            new_text=sql.Literal(new_text))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR

    finally:
        conn.close()
    return ReturnValue.OK



def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:

    if owner_id <= 0 or apartment_id <= 0 or owner_id is None or apartment_id is None:
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("INSERT INTO Owns " +
                        "SELECT {owner_id}, {apartment_id}").format(
                            owner_id=sql.Literal(owner_id),
                            apartment_id=sql.Literal(apartment_id))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.ALREADY_EXISTS

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # where owner or apartment not exist
        print(e)
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        # where apartment is already owned
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR

    finally:
        conn.close()
    return ReturnValue.OK

def owner_doesnt_own_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    if owner_id <= 0 or apartment_id <= 0 or owner_id is None or apartment_id is None:
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("DELETE FROM Owns " +
                        "WHERE id = {apartment_id} AND Owner_id = {owner_id}").format(
                        owner_id=sql.Literal(owner_id),
                        apartment_id=sql.Literal(apartment_id))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS

    except Exception as e:
        print(e)
        return ReturnValue.ERROR

    finally:
        conn.close()
    return ReturnValue.OK


def get_apartment_owner(apartment_id: int) -> Owner:
    returned_owner = Owner.bad_owner()

    if apartment_id <= 0 or apartment_id is None:
        return returned_owner

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("SELECT * FROM Owner O " +
                        "WHERE EXISTS (SELECT 1 FROM Owns " +
                        "WHERE id = {apartment_id} AND O.Owner_ID = Owner_ID)").format(
                        apartment_id=sql.Literal(apartment_id))

        _, result = conn.execute(query)

        if result.size() == 1:
            returned_owner = Owner(**result[0])

    except Exception as e:
        print(e)

    finally:
        conn.close()
    return returned_owner




def get_owner_apartments(owner_id: int) -> List[Apartment]:
    apartments_list = []

    if owner_id <= 0 or owner_id is None:
        return apartments_list

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("SELECT * FROM Apartment AP " +
                        "WHERE EXISTS (SELECT 1 FROM Owns " +
                        "WHERE id = AP.id AND Owner_ID = {owner_id})").format(
                        owner_id=sql.Literal(owner_id))

        _, result = conn.execute(query)

        apartments_list = [Apartment(**apartment) for apartment in result]

    except Exception as e:
        print(e)

    finally:
        conn.close()
    return apartments_list



# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    average = 0

    if apartment_id <= 0 or apartment_id is None:
        return average

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("SELECT average_rating FROM Apartment_Rating " +
                        "WHERE id = {apartment_id}").format(
                        apartment_id=sql.Literal(apartment_id))

        _, result = conn.execute(query)
        print("Came Here")
        print(result)
        if result.size() == 1:
            average = result[0]["average_rating"]
            print("Came Here")

    except Exception as e:
        print(e)

    finally:
        conn.close()
    return average


def get_owner_rating(owner_id: int) -> float:
    average = 0

    if owner_id <= 0 or owner_id is None:
        return average

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("SELECT AVG(average_rating) AS owner_rating FROM Apartment_Rating AR " +
                        "WHERE EXISTS (SELECT 1 FROM Owns " +
                        "WHERE id = AR.id AND owner_id = {owner_id})").format(
                        owner_id=sql.Literal(owner_id))

        _, result = conn.execute(query)

        if result.size() == 1:
            average = result[0]["owner_rating"]

    except Exception as e:
        print(e)

    finally:
        conn.close()
    return average


def get_top_customer() -> Customer:
    customer = Customer.bad_customer()

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("SELECT Customer_id, Customer_name " +
                        "FROM Customer_reservations " +
                        "WHERE num_reservations = (SELECT MAX(num_reservations) FROM Customer_reservations) " +
                        "ORDER BY Customer_id " +
                        "LIMIT 1;").format()

        _, result = conn.execute(query)
        if result.size() == 1:
            customer = Customer(**result[0])

    except Exception as e:
        print(e)

    finally:
        conn.close()
    return customer


def reservations_per_owner() -> List[Tuple[str, int]]:
    result_list = []
    conn = None
    try:
        conn = Connector.DBConnector()

        # total number of reservations to apartments of each owner
        query = sql.SQL("SELECT O.Owner_id, Owner_name, COUNT(R.start_date) AS num_reservations " +
                        "FROM Owner O " +
                        "LEFT OUTER JOIN Owns A ON O.Owner_id = A.Owner_id " +
                        "LEFT OUTER JOIN Reserved R ON A.ID = R.ID " +
                        "GROUP BY O.Owner_id, Owner_name;").format()

        _, result = conn.execute(query)

        if result.size() < 1:
            return result_list
        else:
            for row in result:
                result_list.append((row["Owner_name"], row["num_reservations"]))

    except Exception as e:
        print(e)

    finally:
        conn.close()

    return result_list


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
    # TODO: implement
    # get all location owners
    # in order for that we need to know all the cities where there are apartments
    # and to check on each Owner for each city if he owns an apartment there
    # will to that by counting all the cities and count all owner's cities and comparing them.

    owners_list = []

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("SELECT Owner_id, Owner_name " +
                        "FROM Owner_cities_count " +
                        "WHERE num_cities = " +
                        "(SELECT COUNT(DISTINCT City) " +
                        "FROM Apartment);").format()

        _, result = conn.execute(query)
        if result.size() > 0:
            owners_list = [Owner(**owner) for owner in result]

    except Exception as e:
        print(e)

    finally:
        conn.close()
    return owners_list


def best_value_for_money() -> Apartment:
    # value for money of a given apartment is calculated as follows:
    # the regular average of all review scores

    # review score is calculated as follows:
    # rating / [total_price / (end_date - start_date)]

    # idea:
    # create view of reservations and their reviews (how the fuck?)

    # query the following : the apartment (id and name) that maximized avg(score) on its reviews
    pass

def profit_per_month(year: int) -> List[Tuple[int, float]]:

    start_year_date = date(year, 1, 1)
    end_year_date = date(year, 12, 31)

    profit_list = []

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("SELECT M.month AS month, COALESCE((SUM(R.total_price) * 0.15), 0) AS app_profit FROM Months.M " +
                        "Apartment A LEFT OUTER JOIN Reserved R ON A.ID = R.ID " +
                        "WHERE (R.end_date BETWEEN {start_year_date} AND {end_year_date}) " +
                        "OR (R.end_date IS NULL) " +
                        "GROUP BY A.ID").format(
                            start_year_date=sql.Literal(start_year_date.strftime('%Y-%m-%d')),
                            end_year_date=sql.Literal(end_year_date.strftime('%Y-%m-%d')))

        _, result = conn.execute(query)
        if result.size() > 0:
            profit_list = [(res["app_id"], res["app_profit_per_month"]) for res in result]

    except Exception as e:
        print(e)

    finally:
        conn.close()
    return profit_list


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass