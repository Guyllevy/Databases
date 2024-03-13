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
               "CREATE VIEW Apartment_Rating AS SELECT A.ID, AVG(COALESCE(rating, 0)) AS average_rating FROM Apartment A LEFT OUTER JOIN Reviewed R ON A.ID = R.ID GROUP BY A.ID;",
               "CREATE VIEW Customer_reservations AS SELECT C.Customer_id, C.Customer_name, COUNT(R.start_date) AS num_reservations FROM Customer C LEFT OUTER JOIN Reserved R ON C.Customer_id = R.Customer_id GROUP BY C.Customer_id, C.Customer_name;",
               "CREATE VIEW Apartment_Average_price_per_night AS SELECT RS.ID, AVG(RS.total_price / (RS.end_date - RS.start_date)) as average_ppn FROM Reserved RS GROUP BY RS.ID",
               "CREATE VIEW Apartment_VFM_scores AS SELECT R.ID, COALESCE(average_rating / average_ppn, 0) as score FROM Apartment_Rating R LEFT OUTER JOIN Apartment_Average_price_per_night PPN ON R.ID = PPN.ID",
               "CREATE VIEW Rating_Ratios AS SELECT R1.Customer_id AS cid1 , R2.Customer_id AS cid2, AVG(CAST(R1.rating AS decimal)/R2.rating) AS ratio FROM Reviewed R1, Reviewed R2 WHERE R1.Customer_id != R2.Customer_id AND R1.ID = R2.ID GROUP BY R1.Customer_id, R2.Customer_id",
               # adi
               "CREATE VIEW Owner_cities_count AS (SELECT O.Owner_id, O.Owner_name, COUNT(DISTINCT (A.City, A.Country)) AS num_cities FROM (Owner O LEFT OUTER JOIN Owns OW ON O.Owner_id = OW.Owner_id) LEFT OUTER JOIN Apartment A ON OW.id = A.id GROUP BY O.Owner_id, O.Owner_name);"]


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
               "DROP VIEW Apartment_rating;",
               "DROP VIEW Customer_reservations;",
               "DROP VIEW Apartment_Average_price_per_night;",
               "DROP VIEW Apartment_VFM_scores;",
               "DROP VIEW Rating_Ratios;",
               # adi,
               "DROP VIEW Owner_cities_count;"]

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
    elif apartment.get_size() <= 0:
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
    # TODO: maybe remove checks customer_id > 0 and apartment_id > 0 because of piazza question number @54
    if (customer_id is None or customer_id <= 0 or apartment_id is None or apartment_id <= 0 or total_price is None
            or total_price <= 0 or start_date is None or end_date is None or end_date < start_date):
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("INSERT INTO Reserved " +
                        "SELECT {customer_id}, {apartment_id}, {start_date}, {end_date}, {total_price} " +
                        "WHERE NOT EXISTS (SELECT 1 FROM Reserved AS R " +
                        "WHERE R.id = {apartment_id} AND " +
                        "(R.start_date, R.end_date) OVERLAPS ({start_date}, {end_date}))").format(
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
    if (customer_id is None or customer_id <= 0 or apartment_id is None or apartment_id <= 0
            or review_date is None or review_text is None or rating is None or rating < 1 or rating > 10):
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("INSERT INTO Reviewed " +
                        "SELECT {apartment_id}, {customer_id}, {review_date}, {rating}, {review_text} " +
                        "WHERE EXISTS (SELECT 1 FROM Reserved AS R " +
                        "WHERE R.id = {apartment_id} AND R.Customer_ID = {customer_id} " +
                        "AND R.end_date <= {review_date})").format(
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
                        "AND review_date <= {update_date}").format(
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

def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
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

        if result.size() == 1:
            average = result[0]["average_rating"]

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

        query = sql.SQL("SELECT COALESCE(AVG(average_rating),0) AS owner_rating FROM Apartment_Rating AR " +
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
                        "(SELECT COUNT(DISTINCT (City, Country)) " +
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
    # (average of all the ratings of that apartment) / (average of all reservations prices per night)

    # idea:
    # create view of average ratings of apartments -- *exists* from previous functions (Apartment_Rating)
    # create view of average price per night of apartments                           (Apartment_Average_price_per_night)
    # create view for scores of apartments                                           (Apartment_VFM_scores)
    # query for the apartment that maximizes (average rating / average PPN)

    apartment = Apartment.bad_apartment()

    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("SELECT A.ID, Address, City, Country, Size " +
                        "FROM Apartment A JOIN Apartment_VFM_scores S ON A.ID = S.ID " +
                        "WHERE score = (SELECT MAX(score) FROM Apartment_VFM_scores) " +
                        "ORDER BY A.ID " +
                        "LIMIT 1;").format()

        _, result = conn.execute(query)

        if result.size() == 1:
            apartment = Apartment(**result[0])

    except Exception as e:
        print(e)

    finally:
        conn.close()
    return apartment


def profit_per_month(year: int) -> List[Tuple[int, float]]:

    # the app’s profit from a reservation is 15% from the total price. The function should
    # return the profit per month in the specified year, including months where no profit was
    # made. The month of the reservation should be determined by the end_date.

    profit_list = []
    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("SELECT EXTRACT(MONTH FROM RS.end_date) AS month, SUM(total_price)*0.15 AS profit " +
                        "FROM ("
                        "(SELECT * FROM Reserved WHERE EXTRACT(YEAR FROM end_date) = {year}) " +
                        "UNION " +
                        "(SELECT 0 AS Customer_id, 0 AS ID, DATE('{year}-01-01') AS start_date, generate_series(DATE('{year}-01-01'), DATE('{year}-12-01'), '1 month') AS end_date, 0 AS total_price)" +
                        ") RS " +
                        "GROUP BY EXTRACT(MONTH FROM RS.end_date) "
                        "ORDER BY EXTRACT(MONTH FROM RS.end_date)").format(year=sql.Literal(year))

        _, result = conn.execute(query)

        if result.size() >= 1:
            for row in result:
                profit_list.append(
                    ((row["month"]), row["profit"]))

    except Exception as e:
        print(e)

    finally:
        conn.close()
    return profit_list





def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:

    # say C is the customer we want to give recommendations to.
    # for every customer which is not C.
    # if they have a common apartment which they rated : we consider the ratio (average of ratios) of their ratings
    # then for every apartment A not already reviewed by C, but reviewed by some other customer with now known ratio,
    # we approximate the rating of A using costumer OC by multiplying OC_rating by the ratio C/OC
    # this potentially gives multiple approximations per apartment, so we average over that.
    #
    # view (Rating_Ratios):
    # view of customer pairs that have reviewed
    # a common apartment, and the (average) ratio of their ratings
    #
    # main query:
    # the join looks like (cid1, cid2, ratio, rating, apartment)
    # cid1 is our customer of interest,
    # cid2 is all customers which we have a ratio for and reviewed an apartment (which is not already reviewed by cid1),
    # apartment is that an apartment that cid2 reviewed
    # ratio is the average ratio cid1/cid2 ratings
    # rating is the rating of apartment by cid2
    #
    # where:
    # cid1 is (as we said) our customer of interest,
    # the apartment shown were NOT reviewed by cid1 already,
    #
    # group by: apartment
    # and average the approximations for each cid2-given information (each of which we make sure is between 1 and 10)

    result_list = []
    conn = None
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("SELECT A.ID AS ID, Address, City, Country, Size, " +
                        "AVG(GREATEST(LEAST(RR.ratio * RE.rating, 10), 1)) AS approx " +
                        "FROM Apartment A " +
                        "JOIN Reviewed RE ON A.ID = RE.ID " +
                        "JOIN Rating_Ratios RR ON RR.cid2 = RE.Customer_id " +
                        "WHERE RR.cid1 = {Customer_id} " +
                        "AND NOT EXISTS (SELECT * FROM Reviewed WHERE ID = A.ID AND Customer_id = {Customer_id}) " +
                        "GROUP BY A.ID, Address, City, Country, Size " +
                        "ORDER BY A.ID").format(Customer_id=sql.Literal(customer_id))

        _, result = conn.execute(query)

        if result.size() >= 1:
            for row in result:
                result_list.append(
                    (Apartment(row["ID"], row["Address"], row["City"], row["Country"], row["Size"]), float(row["approx"])))

    except Exception as e:
        print(e)

    finally:
        conn.close()
    return result_list


# print table for debugging purposes.
def get_table(query_string) -> None:
    conn = Connector.DBConnector()
    query = sql.SQL(query_string).format()
    _, result = conn.execute(query)
    if result.size() < 1:
        print("result given to get_table is none")
        return
    for field in result[0]:
        print(field, end=" - ")
    print()
    for row in result:
        for field in row:
            print(row[field], end=' | ')
        print()
    return
