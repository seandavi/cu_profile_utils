from dataclasses import dataclass, asdict
import httpx
from bs4 import BeautifulSoup, Tag
import pandas as pd


def get_td_text(td_element: Tag) -> str:
    """Utility function to get the text from a td element"""
    return td_element.text.strip()


@dataclass
class SearchTableRow:
    """A row of data from a profile page table"""

    name: str
    institution: str
    department: str
    title: str
    profile_id: int


def create_profile_table_row_from_tds(columns: list[Tag]) -> SearchTableRow:
    """Create a ProfileTableRow from a list of td elements"""
    return SearchTableRow(
        name=get_td_text(columns[0]),
        institution=get_td_text(columns[1]),
        department=get_td_text(columns[2]),
        title=get_td_text(columns[3]),
        profile_id=int(columns[0].find("a")["href"].split("/")[-1]),  # type: ignore
    )


class SearchPage:
    """Represents a single Profile Search Page

    Usually, one will want to loop over these pages
    to get all the data.
    """

    def __init__(self, page_size: int = 100, page=1):
        """Create a new ProfileSearch

        page is (one-based) and default page_size of 100
        is the maximum allowed by the site.
        """
        self.page_size = page_size
        self.page = page
        self._get_search_page_soup()
        self._get_profile_table_data()

    def get_results(self) -> pd.DataFrame:
        """Return the data as a list of `ProfileTableRow` objects"""
        return self._data

    # perform http request to get the page and
    # parse to a BeautifulSoup object
    def _get_search_page_soup(self):
        page = self.page
        page_size = self.page_size
        url = f"https://profiles.ucdenver.edu/search/default.aspx?searchtype=people&searchfor=&exactphrase=false&perpage={page_size}&offset=0&page={page}&totalpages=569&searchrequest=A81BSfTwU3GNm4liSODkW6vB3EBYO6gz+a5TY1bFhuz1tc7ngL4Orww3064KoquGaRdozjhWRGlrnur5IbaEcMH3TeE05jmp/c7agcYTrzG/rrN5T5p39rbdUtWdCA0xO6jz/+zNo8xTen6DVgqqi0W/y1wHaBbEaTD7d+ObAfEiPSt4sYkjfpHHCVWp3IgQjZuJYkjg5FtrbjF9BEDCXidTb5mQuzDHyB9Btw8xWu0u+sg0NH5oV8eO5TZfqG6zAJei5w7JqjuiyOFytEGpzItfRAJL6BXOyZmTRCU0RtrqmPKU0fOLSVV35kew5OQiQnv3EOh+q7Y=&sortby=&sortdirection=&showcolumns=11"
        response = httpx.get(url, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")
        self._soup = soup

    # Gets the rows of the table that contains the search results
    # and returns a list of ProfileTableRow objects
    def _get_tbl_srch_result_rows(self):
        table = self._soup.find("table", {"id": "tblSearchResults"})
        rows = table.find_all("tr")  # type: ignore
        return rows

    # parse the rows into a list of ProfileTableRow objects
    def _get_profile_table_data(self):
        rows = self._get_tbl_srch_result_rows()
        data = []
        for row in rows:
            cols = row.find_all("td")
            if len(cols) == 0:
                continue
            table_row = create_profile_table_row_from_tds(cols)
            data.append(table_row)
        self._data = pd.DataFrame([asdict(record) for record in data])

    def has_next_page(self):
        return self._soup.find("a", {"href": "javascript:GotoNextPage();"}) is not None
