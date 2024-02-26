import unittest
import Solution as Solution
from Utility.ReturnValue import ReturnValue
from Tests.AbstractTest import AbstractTest

from Business.Apartment import Apartment
from Business.Owner import Owner
from Business.Customer import Customer

'''
    Simple test, create one of your own
    make sure the tests' names start with test
'''


class Test(AbstractTest):
    def test_customer(self) -> None:
        c1 = Customer(1, 'much customer')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'regular customer')
        c2 = Customer(2, None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c2), 'invalid name')
        print(Solution.get_customer(1))
        o1 = Owner(1, 'very owner')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o1), 'regular owner')
        print(Solution.get_owner(1))
        a1 = Apartment(1, 'very address', 'much city', 'much state', 1000)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(a1), 'regular apartment')
        print(Solution.get_apartment(1))
        print(Solution.get_apartment(1).get_size())
        #delete owner
        self.assertEqual(ReturnValue.OK, Solution.delete_owner(1), 'regular owner')
        self.assertEqual(ReturnValue.OK, Solution.delete_apartment(1), 'regular apartment')


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
