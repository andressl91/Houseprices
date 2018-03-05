import unittest as ut

from ap.model.realestate_model import RealEstate
from ap.model.realestate_model import Feature as fe

class TestRealEstate(ut.TestCase):

    def test_initiate_and_update(self):
        fe_dict = {fe.ID.value: "123", fe.PRICE.value: "120000"}
        re = RealEstate(fe_dict)
        self.assertEqual(re.get_base_info(), fe_dict)

        fe_update = {fe.SQ_M.value: "90"}
        fe_dict.update(fe_update)
        re.update_features(fe_update)
        self.assertEqual(fe_dict, re.get_base_info())

if __name__ == "__main__":
    # Simple
    #ut.main()

    # Verbose
    suite = ut.TestLoader().loadTestsFromTestCase(TestRealEstate)
    ut.TextTestRunner(verbosity=3).run(suite)