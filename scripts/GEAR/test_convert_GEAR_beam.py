import convert_GEAR_beam
import unittest
import shutil
import zarr
import numpy as np

config_file_path = "config/test.yaml"
test_data_output = "test_data/output.zarr"
expected_data_output = "test_data/expected.zarr"

class TestConvertsToZarr(unittest.TestCase):
    def setUp(self):
        # delete zarr if exists
        try:
            shutil.rmtree(test_data_output)
        except FileNotFoundError:
            pass

    def tearDown(self):
        # delete zarr if exists
        try:
            shutil.rmtree(test_data_output)
        except FileNotFoundError:
            pass

    def test_convert_to_zarr(self):
        # convert test data to zarr
        convert_GEAR_beam.main(config_file_path)

        # read in resulting zarr file
        result = zarr.open(test_data_output, mode='r')

        # read in expected zarr file
        expected = zarr.open(expected_data_output, mode='r')

        # compare result and expected zarr
        timeCheck = np.all(result['time'] == expected['time'])
        yCheck = np.all(result['y'] == expected['y'])
        xCheck = np.all(result['x'] == expected['x'])

        self.assertTrue(timeCheck)
        self.assertTrue(yCheck)
        self.assertTrue(xCheck)
 
if __name__ == "__main__":
    unittest.main()
