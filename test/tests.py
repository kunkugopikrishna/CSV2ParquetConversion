import os
import shutil
import unittest

from constants import input_csv_dir_path
from main import main


class Test(unittest.TestCase):
    input_dir = "test\input_dir"
    output_dir = "test\output_dir"

    # Test case for missing input dir
    def test_no_input_dir(self):
        if os.path.exists(self.input_dir):
            os.rmdir(self.input_dir)

        output = main(self.input_dir, self.output_dir)
        self.assertEqual(output, "Input Path doesn't exists")

    # Test case for empty input dir
    def test_no_input_files(self):
        # Make Empty dir to test no files
        if not os.path.exists(self.input_dir):
            os.makedirs(self.input_dir)
        else:
            os.rmdir(self.input_dir)
            os.makedirs(self.input_dir)

        output = main(self.input_dir, self.output_dir)
        self.assertEqual(output, "No files found for processing")

    # Test case for valid input files
    def test_valid_input_files(self):
        # Create output dir if not exists
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        output = main(input_csv_dir_path, self.output_dir)
        self.assertEqual(output, "Successfully Processed All Files")

        # Test for output file generation
        self.assertGreaterEqual(len(os.listdir(self.output_dir)), 1)

    # Remove Test Directories and content, after all test cases
    def tearDown(self):
        if os.path.exists(self.input_dir):
            shutil.rmtree(self.input_dir)
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)


if __name__ == '__main__':
    unittest.main()

