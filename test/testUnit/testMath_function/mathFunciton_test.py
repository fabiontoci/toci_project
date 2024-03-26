import unittest


def square(number):
    """
    This function calculates the square of a given number.

    :param number: The number to calculate the square of.
    :type number: int or float
    :return: The square of the given number.
    :rtype: int or float
    """
    return number ** 2


class TestSquareFunction(unittest.TestCase):
    def test_positive_number(self):
        self.assertEqual(square(5), 25)
        self.assertEqual(square(10), 100)
        self.assertEqual(square(3.5), 12.25)

    def test_negative_number(self):
        self.assertEqual(square(-5), 25)
        self.assertEqual(square(-10), 100)
        self.assertEqual(square(-3.5), 12.25)

    def test_zero(self):
        self.assertEqual(square(0), 0)


if __name__ == '__main__':
    unittest.main()
