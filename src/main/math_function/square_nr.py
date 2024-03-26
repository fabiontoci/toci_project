def square(number):
    """
    This function calculates the square of a given number.

    :param number: The number to calculate the square of.
    :type number: int or float
    :return: The square of the given number.
    :rtype: int or float
    """
    return number ** 2


# Example of using the function
number_to_calculate = 5
result = square(number_to_calculate)
print("The square of", number_to_calculate, "is", result)
