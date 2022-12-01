from cu_profile_utils import search_page


def test_search_page():
    PAGE_SIZE = 15
    searcher = search_page.SearchPage(page=1, page_size=PAGE_SIZE)
    assert searcher.has_next_page()
    assert searcher.get_results().shape == (PAGE_SIZE, 5)
    assert searcher.get_results().columns.tolist() == [
        "name",
        "institution",
        "department",
        "title",
        "profile_id",
    ]

    searcher = search_page.SearchPage(page=100000, page_size=PAGE_SIZE)
    assert not searcher.has_next_page()
