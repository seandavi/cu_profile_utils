from cu_profile_utils import profile_page
import pytest

# See https://anyio.readthedocs.io/en/stable/testing.html#specifying-the-backends-to-run-on
@pytest.mark.parametrize("anyio_backend", ["asyncio"])
async def test_profile_page_pmids(anyio_backend):
    profile = profile_page.ProfilePage(31093281)
    pmid_df = await profile.get_pmids_as_dataframe()
    assert pmid_df.shape[0] > 1
    assert pmid_df.shape[1] == 2


@pytest.mark.parametrize("anyio_backend", ["asyncio"])
async def test_profile_page_mesh_sims(anyio_backend):
    profile = profile_page.ProfilePage(31093281)
    mesh_df = await profile.get_mesh_term_similarities_as_dataframe()
    assert mesh_df.shape[0] > 1
    assert mesh_df.shape[1] == 6
