import logging

import anyio
import pandas as pd
import structlog
from anyio import CapacityLimiter, create_task_group
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from .profile_page import ProfilePage
from .search_page import SearchPage

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False,
)

logger = structlog.get_logger()


mesh_map = {}
pmid_map = {}


def get_profile_table(start_page: int = 1) -> pd.DataFrame:
    page = start_page
    df_list = []
    logger.info("starting search page", extra={"page": page})
    try:
        searcher = SearchPage(page=page)
    except:
        logger.error("Error creating SearchPage object")
        raise
    df_list.append(searcher.get_results())
    while searcher.has_next_page():
        page += 1
        logger.info("parsing search page", extra={"page": page})
        searcher = SearchPage(page=page)
        df = searcher.get_results()
        df_list.append(df)
    return pd.concat(df_list)


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=0.5, max=2))
async def pubmed_ids(profile_id, limiter):
    async with limiter:
        profile = ProfilePage(profile_id)
        pmids = await profile.get_pmids_as_dataframe()
        mesh_sims = await profile.get_mesh_term_similarities_as_dataframe()
        pmid_map[profile_id] = pmids
        mesh_map[profile_id] = mesh_sims
        if len(pmid_map) % 50 == 0:
            logger.info("Parsing profiles", extra={"current_count": len(pmid_map)})


async def get_pubmed_ids(profile_ids: list[int], api_limit: int = 10):
    limiter = CapacityLimiter(10)
    async with create_task_group() as tg:
        for profile_id in profile_ids:
            tg.start_soon(pubmed_ids, profile_id, limiter)


if __name__ == "__main__":
    logger.info("Starting")
    vals = get_profile_table(start_page=1)
    vals.to_csv("profile_table.csv", index=False)
    logger.info(
        "saved profile table to file profile_table.csv",
        extra={"filename": "profile_table.csv"},
    )

    logger.info("Finished parsing profile table")

    logger.info("Starting to parse profiles")
    profile_ids = vals["profile_id"].tolist()
    anyio.run(get_pubmed_ids, profile_ids, 15)

    pd.concat(pmid_map.values()).to_csv("profile_pmids.csv", index=False)
    logger.info(
        "saved profile pmids to file profile_pmids.csv",
        extra={"filename": "profile_pmids.csv"},
    )
    pd.concat(mesh_map.values()).to_csv("profile_mesh_sims.csv", index=False)
    logger.info(
        "saved profile mesh sims to file profile_mesh_sims.csv",
        extra={"filename": "profile_mesh_sims.csv"},
    )

    logger.info("Finished parsing profiles")

    logger.info("Finished")
