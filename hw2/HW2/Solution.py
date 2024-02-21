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
    queries = ["CREATE TABLE Owner(Owner_ID INTEGER , Name TEXT, PRIMARY KEY(Owner_ID), CHECK(ID > 0));",
               "CREATE TABLE Apartment(ID INTEGER, Address TEXT, City TEXT, Country TEXT, Size INTEGER,UNIQUE(City, Address) PRIMARY KEY(ID), CHECK(ID > 0));",
               "CREATE TABLE Customer(Customer_ID INTEGER, Customer_name TEXT, PRIMARY KEY(Customer_ID), CHECK(ID > 0));",
               "CREATE TABLE Owns(Owner_ID INTEGER, ID INTEGER, FOREIGN KEY(ID) REFERENCES Apartment(ID) ON DELETE CASCADE);",
               "CREATE TABLE Reserved(Customer_ID INTEGER, ID INTEGER, start_date DATE, end_date DATE, total_price FLOAT, FOREIGN KEY(ID) REFERENCES Apartment ON DELETE CASCADE, FOREIGN KEY(Customer_ID) REFERENCES Customer ON DELETE CASCADE);",
               "CREATE TABLE Reviewed(ID INTEGER, Customer_ID INTEGER, review_date DATE, rating INTEGER, review_text TEXT, FOREIGN KEY(ID) REFERENCES Apartment ON DELETE CASCADE, FOREIGN KEY(Customer_ID) REFERENCES Customer ON DELETE CASCADE);",
               "CREATE VIEW Apartment_rating AS SELECT ID, AVG(rating) AS total_rating FROM Reviewed GROUPBY ID;"]

    conn = None
    try:
        conn = Connector.DBConnector()
        for query in queries:
            conn.execute(query)

    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

def clear_tables():
    # TODO: implement
    pass


def drop_tables():
    # TODO: implement
    pass


def add_owner(owner: Owner) -> ReturnValue:
    # TODO: implement
    pass


def get_owner(owner_id: int) -> Owner:
    # TODO: implement
    pass


def delete_owner(owner_id: int) -> ReturnValue:
    # TODO: implement
    pass


def add_apartment(apartment: Apartment) -> ReturnValue:
    # TODO: implement
    pass


def get_apartment(apartment_id: int) -> Apartment:
    # TODO: implement
    pass


def delete_apartment(apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def add_customer(customer: Customer) -> ReturnValue:
    # TODO: implement
    pass


def get_customer(customer_id: int) -> Customer:
    # TODO: implement
    pass


def delete_customer(customer_id: int) -> ReturnValue:
    # TODO: implement
    pass


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
    # TODO: implement
    pass


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
    #SELECT Avg(rating) FROM review R WHERE R.ID = {apartment id}
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
