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
               "CREATE TABLE Apartment(ID INTEGER, Address TEXT, City TEXT, Country TEXT, Size INTEGER,UNIQUE(City, Address), PRIMARY KEY(ID), CHECK(ID > 0));",
               "CREATE TABLE Customer(Customer_ID INTEGER, Customer_name TEXT, PRIMARY KEY(Customer_ID), CHECK(Customer_ID > 0));",
               "CREATE TABLE Owns(Owner_ID INTEGER, ID INTEGER, FOREIGN KEY(ID) REFERENCES Apartment(ID) ON DELETE CASCADE);",
               "CREATE TABLE Reserved(Customer_ID INTEGER, ID INTEGER, start_date DATE, end_date DATE, total_price FLOAT, FOREIGN KEY(ID) REFERENCES Apartment ON DELETE CASCADE, FOREIGN KEY(Customer_ID) REFERENCES Customer ON DELETE CASCADE);",
               "CREATE TABLE Reviewed(ID INTEGER, Customer_ID INTEGER, review_date DATE, rating INTEGER, review_text TEXT, FOREIGN KEY(ID) REFERENCES Apartment ON DELETE CASCADE, FOREIGN KEY(Customer_ID) REFERENCES Customer ON DELETE CASCADE);",
               "CREATE VIEW Apartment_rating AS SELECT ID, AVG(rating) AS total_rating FROM Reviewed GROUP BY ID;"]

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
               "DELETE FROM Reserved;",
               "DELETE FROM Reviewed;"]
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
               "DROP VIEW Apartment_rating;"]
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
    if customer_id <= 0 or customer_id is None or apartment_id <= 0 or apartment_id is None or total_price <= 0\
            or start_date is None or end_date is None or total_price is None:
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("INSERT INTO Reserved "+
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
    # TODO: implement
    pass


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    # TODO: implement
    pass


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def owner_doesnt_own_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def get_apartment_owner(apartment_id: int) -> Owner:
    # TODO: implement
    pass


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    # TODO: implement
    # SELECT Avg(rating) FROM review R WHERE R.ID = {apartment id}
    pass


def get_owner_rating(owner_id: int) -> float:
    # TODO: implement
    pass


def get_top_customer() -> Customer:
    # TODO: implement
    pass


def reservations_per_owner() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
    # TODO: implement
    pass


def best_value_for_money() -> Apartment:
    # TODO: implement
    pass


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass
