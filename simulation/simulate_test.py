import unittest
from simulate import Tree


class TestTree(unittest.TestCase):
    def test_get_random_paths_is_in_range(self):
        tree = Tree(tree_path_count=10)
        batch_size = 5
        paths = tree.get_random_paths(batch_size)
        for path in paths:
            self.assertTrue(1 <= path <= 10)

    def test_get_random_paths_returns_correct_number_of_paths(self):
        tree = Tree(tree_path_count=10)
        batch_size = 5
        paths = tree.get_random_paths(batch_size)
        self.assertEqual(len(paths), batch_size)

    def test_get_unique_buckets_for_random_paths_without_duplicates(self):
        tree = Tree(tree_path_count=2)
        #                 1
        #                / \
        #               2   3
        # path number:  1   2
        buckets = tree.get_unique_buckets_for_random_paths(random_paths=[1])
        expected_buckets = {1, 2}
        self.assertSetEqual(buckets, expected_buckets)
    
    def test_get_unique_buckets_for_random_paths_with_duplicates(self):
        tree = Tree(tree_path_count=2)
        #                 1
        #                / \
        #               2   3
        # path number:  1   2
        buckets = tree.get_unique_buckets_for_random_paths(random_paths=[1, 2])
        expected_buckets = {1, 2, 3}
        self.assertSetEqual(buckets, expected_buckets)
    
    def test_get_unique_buckets_for_random_paths_with_multiple_paths(self):
        tree = Tree(tree_path_count=4)
        #                    1
        #                  /   \
        #                 2     3
        #                / \   / \
        #               4   5 6   7
        # path number:  1   2 3   4
        buckets = tree.get_unique_buckets_for_random_paths(random_paths=[1, 2, 3])
        expected_buckets = {1, 2, 4, 5, 3, 6}
        self.assertSetEqual(buckets, expected_buckets)

if __name__ == '__main__':
    unittest.main()