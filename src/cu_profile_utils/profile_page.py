import httpx
from bs4 import BeautifulSoup, Tag
from dataclasses import dataclass
from typing import Any
import pandas as pd


def fix_mesh_table_raw_row_vals(raw_row_vals) -> dict[str, Any]:
    """Fix the raw row values from the mesh table"""
    return {
        "mesh_term": raw_row_vals[0],
        "publication_count": int(raw_row_vals[1]),
        "most_recent_publication_year": int(raw_row_vals[2]),
        "publication_count_all_authors": int(raw_row_vals[3]),
        "concept_score": float(raw_row_vals[4]),
    }


async def get_mesh_term_similarity_page(profile_id: int) -> BeautifulSoup:
    async with httpx.AsyncClient() as client:
        url = f"https://profiles.ucdenver.edu/display/{profile_id}/network/researchareas/details"
        response = await client.get(url, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")
        return soup


async def get_profile_page(profile_id: int) -> BeautifulSoup:
    async with httpx.AsyncClient() as client:
        url = f"https://profiles.ucdenver.edu/display/{profile_id}"
        response = await client.get(url, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")
        return soup


def get_pmids_as_dataframe(self, include_profile_id=True) -> "pd.DataFrame":
    """Return a dataframe of PubMed IDs for the profile"""
    import pandas as pd

    return pd.DataFrame(self.get_pmids_as_list(), columns=["pmid"])


class ProfilePage:
    """Represents a single profile for a member of the CU community

    The main purpose of this class is to provide a way to get the PMIDs for a
    given profile.
    """

    def __init__(self, profile_id: int):
        """Initialize the ProfilePage object

        The profile_id is available as a public attribute.
        """
        self.profile_id = profile_id
        self._pmids: list[str] = None  # type: ignore
        self._mesh_terms: list[dict[str, Any]] = None  # type: ignore

    async def _get_profile_page_publications(self) -> None:
        soup = await get_profile_page(self.profile_id)
        pub_div = soup.find("div", {"id": "publicationListAll"})
        self._pmids = []
        if pub_div is None:
            return
        lis = pub_div.find_all("li")  # type: ignore
        for pub in lis:
            pmid = pub.get("data-pmid", None)
            if pmid is not None:
                self._pmids.append(pmid)

    async def get_pmids_as_list(self) -> list[str]:
        """Return a list of PubMed IDs for the profile"""
        if self._pmids is None:
            await self._get_profile_page_publications()
        return self._pmids

    async def get_pmids_as_dataframe(self, include_profile_id=True) -> "pd.DataFrame":
        """Return a dataframe of PubMed IDs for the profile"""
        pmid_list = await self.get_pmids_as_list()
        if include_profile_id:
            return pd.DataFrame(
                [{"profile_id": self.profile_id, "pmid": pmid} for pmid in pmid_list]
            )
        else:
            return pd.DataFrame(pmid_list, columns=["pmid"])

    async def _get_mesh_term_similarity_table(self):
        soup = await get_mesh_term_similarity_page(self.profile_id)
        table = soup.find("table", {"id": "thetable1"})
        if table is None:
            self._mesh_terms = []
            return
        rows = []
        for row in table.find_all("tr"):  # type: ignore
            cells = row.find_all("td")
            if len(cells) == 0:
                continue
            raw_row_vals = list([cell.text for cell in cells])
            rows.append(fix_mesh_table_raw_row_vals(raw_row_vals))
        self._mesh_terms = rows

    async def get_mesh_term_similarities_as_dict(self, include_profile_id=True):
        """Return a list of mesh terms and their similarity scores"""
        await self._get_mesh_term_similarity_table()
        if len(self._mesh_terms) == 0:
            return []
        if include_profile_id:
            return [{"profile_id": self.profile_id, **row} for row in self._mesh_terms]
        else:
            return self._mesh_terms

    async def get_mesh_term_similarities_as_dataframe(self, include_profile_id=True):
        """Return a dataframe of mesh terms and their similarity scores"""
        mesh_dict = await self.get_mesh_term_similarities_as_dict()
        if len(mesh_dict) == 0:
            return pd.DataFrame()
        return pd.DataFrame(mesh_dict)
