import unittest
"""
soon
"""
def sum(a,b):
    return a+b

class Test(unittest.TestCase):
    def setUp(self):
        self.a = 10
        self.b = 20
    def tearDown(self):
        self.a = 0
        self.b = 0
        print("test attributes cleared..")

    def test_sum_1(self):

        # act
        result = sum(self.a,self.b)
        # assert
        self.assertEqual(result,self.a + self.b)

    def test_sum_2(self):

        # act
        result = sum(self.a,self.b)
        # assert
        self.assertEqual(result,self.a + self.b)

# try:
#     if __name__=="__main__":

unittest.main(argv=[''], verbosity=3, exit=False)
