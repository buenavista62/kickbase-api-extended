import json
import os

import pytest

from kickbase_api.kickbase import Kickbase


def create_logged_in_kickbase():
    kickbase = Kickbase()
    user, leagues = kickbase.login(os.environ["KKBS_TEST_USERNAME"], os.environ["KKBS_TEST_PASSWORD"])
    return kickbase, user, leagues


@pytest.fixture(scope="module")
def logged_in_kickbase():
    return create_logged_in_kickbase()
