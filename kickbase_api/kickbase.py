from datetime import datetime, timedelta, timezone
from typing import Union
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from kickbase_api.exceptions import KickbaseLoginException, KickbaseException
from kickbase_api.models._transforms import parse_date, date_to_string
from kickbase_api.models.chat_item import ChatItem
from kickbase_api.models.feed_item import FeedItem
from kickbase_api.models.feed_item_comment import FeedItemComment
from kickbase_api.models.gift import Gift
from kickbase_api.models.league_data import LeagueData
from kickbase_api.models.league_info import LeagueInfo
from kickbase_api.models.league_me import LeagueMe
from kickbase_api.models.league_user_data import LeagueUserData
from kickbase_api.models.league_user_profile import LeagueUserProfile
from kickbase_api.models.league_user_stats import LeagueUserStats
from kickbase_api.models.lineup import LineUp
from kickbase_api.models.market import Market
from kickbase_api.models.market_player import MarketPlayer
from kickbase_api.models.player import Player
from kickbase_api.models.response.league_stats_response import LeagueStatsResponse
from kickbase_api.models.user import User


class Kickbase:
    base_url: str = None
    token: str = None
    token_expire: datetime = None
    firebase_token: str = None
    firebase_token_expire: datetime = None
    user: User = None

    _username: str = None
    _password: str = None

    def __init__(
        self,
        base_url: str = "https://api.kickbase.com",
        firestore_project: str = "kickbase-bdb0f",
        google_identity_toolkit_api_key: str = None,
    ):
        self.base_url = base_url
        self.firestore_project = firestore_project
        self.google_identity_toolkit_api_key = google_identity_toolkit_api_key
        # Hinzufügen einer wiederverwendbaren Session:
        self.session = requests.Session()

    def login(self, username: str, password: str) -> (User, [LeagueData]):
        data = {"email": username, "password": password, "ext": False}

        r = self._do_post("/user/login", data, False)

        if r.status_code == 200:
            j = r.json()
            self.token = j["token"]
            self.token_expire = parse_date(j["tokenExp"])

            self._username = username
            self._password = password

            self.user = User(j["user"])
            league_data = [LeagueData(d) for d in j["leagues"]]
            return self.user, league_data

        elif r.status_code == 401:
            raise KickbaseLoginException()
        else:
            raise KickbaseException()

    def _is_token_valid(self):
        if self.token is None or self.token_expire is None:
            return False
        return self.token_expire > datetime.now(timezone.utc) - timedelta(days=1)

    def _is_firebase_token_valid(self):
        if self.firebase_token is None or self.firebase_token_expire is None:
            return False
        return self.firebase_token_expire > datetime.now(timezone.utc) - timedelta(
            minutes=5
        )

    def leagues(self) -> [LeagueData]:
        r = self._do_get("/leagues/", True)

        if r.status_code == 200:
            j = r.json()
            return [LeagueData(d) for d in j["leagues"]]
        else:
            raise KickbaseException()

    def league_me(self, league: Union[str, LeagueData]) -> LeagueMe:
        league_id = self._get_league_id(league)

        r = self._do_get("/leagues/{}/me".format(league_id), True)

        if r.status_code == 200:
            return LeagueMe(r.json())
        else:
            raise KickbaseException()

    def league_info(self, league: Union[str, LeagueData]) -> LeagueInfo:
        league_id = self._get_league_id(league)

        r = self._do_get("/leagues/{}/info".format(league_id), True)

        if r.status_code == 200:
            return LeagueInfo(r.json())
        else:
            raise KickbaseException()

    def league_stats(self, league: Union[str, LeagueData]) -> LeagueStatsResponse:
        league_id = self._get_league_id(league)

        r = self._do_get("/leagues/{}/stats".format(league_id), True)

        if r.status_code == 200:
            return LeagueStatsResponse(r.json())
        else:
            raise KickbaseException()

    def league_users(self, league: Union[str, LeagueData]) -> [LeagueUserData]:
        league_id = self._get_league_id(league)

        r = self._do_get("/leagues/{}/users".format(league_id), True)

        if r.status_code == 200:
            return [LeagueUserData(d) for d in r.json()["users"]]
        else:
            raise KickbaseException()

    def league_user_stats(
        self, league: Union[str, LeagueData], user: Union[str, User]
    ) -> LeagueUserStats:
        league_id = self._get_league_id(league)
        user_id = self._get_user_id(user)

        r = self._do_get("/leagues/{}/users/{}/stats".format(league_id, user_id), True)

        if r.status_code == 200:
            return LeagueUserStats(r.json())
        else:
            raise KickbaseException()

    def league_user_profile(
        self, league: Union[str, LeagueData], user: Union[str, User]
    ) -> LeagueUserProfile:
        league_id = self._get_league_id(league)
        user_id = self._get_user_id(user)

        r = self._do_get(
            "/leagues/{}/users/{}/profile".format(league_id, user_id), True
        )

        if r.status_code == 200:
            return LeagueUserProfile(r.json())
        else:
            raise KickbaseException()

    def league_feed(
        self, start_index: int, league: Union[str, LeagueData]
    ) -> [FeedItem]:
        league_id = self._get_league_id(league)

        r = self._do_get(
            "/leagues/{}/feed?start={}".format(league_id, start_index), True
        )

        if r.status_code == 200:
            return [FeedItem(v) for v in r.json()["items"]]
        else:
            raise KickbaseException()

    def post_feed_item(self, comment: str, league: Union[str, LeagueData]):
        league_id = self._get_league_id(league)

        data = {"comment": comment}

        r = self._do_post("/leagues/{}/feed".format(league_id), data, True)

        if r.status_code == 200:
            return
        else:
            raise KickbaseException()

    def league_feed_comments(
        self, league: Union[str, LeagueData], feed_item: Union[str, FeedItem]
    ) -> [FeedItemComment]:
        league_id = self._get_league_id(league)
        feed_item_id = self._get_feed_item_id(feed_item)

        r = self._do_get(
            "/leagues/{}/feed/{}/comments".format(league_id, feed_item_id), True
        )

        if r.status_code == 200:
            return [FeedItemComment(v) for v in r.json()["comments"]]
        else:
            raise KickbaseException()

    def post_feed_comment(
        self,
        comment: str,
        league: Union[str, LeagueData],
        feed_item: Union[str, FeedItem],
    ):
        league_id = self._get_league_id(league)
        feed_item_id = self._get_feed_item_id(feed_item)

        data = {"comment": comment}

        r = self._do_post(
            "/leagues/{}/feed/{}/comments".format(league_id, feed_item_id), data, True
        )

        if r.status_code == 200:
            return
        else:
            raise KickbaseException()

    def league_user_players(
        self, league: Union[str, LeagueData], user: Union[str, User], match_day: int = 0
    ) -> [Player]:
        league_id = self._get_league_id(league)
        user_id = self._get_user_id(user)

        r = self._do_get(
            "/leagues/{}/users/{}/players?matchDay={}".format(
                league_id, user_id, match_day
            ),
            True,
        )

        if r.status_code == 200:
            return [Player(v) for v in r.json()["players"]]
        else:
            raise KickbaseException()

    def league_collect_gift(self, league: Union[str, LeagueData]) -> True:
        league_id = self._get_league_id(league)

        r = self._do_post("/leagues/{}/collectgift".format(league_id), {}, True)

        if r.status_code == 200:
            return True
        elif r.status_code == 400:
            return False
        else:
            raise KickbaseException()

    def league_current_gift(self, league: Union[str, LeagueData]) -> Gift:
        league_id = self._get_league_id(league)

        r = self._do_get("/leagues/{}/currentgift".format(league_id), True)

        if r.status_code == 200:
            return Gift(r.json())
        else:
            raise KickbaseException()

    def search_player(self, search_query: str) -> [Player]:
        r = self._do_get("/competition/search?t={}".format(search_query), True)

        if r.status_code == 200:
            return [Player(v) for v in r.json()["p"]]
        else:
            raise KickbaseException()

    def team_players(self, team_id: str) -> [Player]:
        r = self._do_get("/competition/teams/{}/players".format(team_id), True)

        if r.status_code == 200:
            return [Player(v) for v in r.json()["p"]]
        else:
            raise KickbaseException()

    """    
    def fetch_players_in_range(self, start, end):
        url = f"/competition/search?start={start}&end={end}"
        r = self._do_get(url, True)
        if r.status_code == 200:
            return [Player(v) for v in r.json()["p"]]
        else:
            raise KickbaseException()

    def fetch_user_id(self, player, league_id):
        url = f"/leagues/{league_id}/players/{player.id}"
        r = self._do_get(url, True)
        if r.status_code == 200:
            user_id = r.json().get("userId")
            if user_id is not None:
                player.user_id = user_id
        return player

    def get_all_players(self, league_id) -> [Player]:
        players = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            ranges = [(i, i + 24) for i in range(0, 700, 25)]
            player_lists = executor.map(lambda r: self.fetch_players_in_range(*r), ranges)
            players = [player for player_list in player_lists for player in player_list]
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            players = list(executor.map(lambda p: self.fetch_user_id(p, league_id), players))

        return players """

    def get_player_market_value_last_n_days(
        self, league_id: str, player_id: str, days: int = 30
    ) -> dict:
        """Get the market value of a player from the last n days.

        Parameters:
        - league_id (str): The ID of the league.
        - player_id (str): The ID of the player.
        - days (int): Number of days to retrieve the market value for (default is 30 days, max is 360 days).

        Returns:
        - dict: Dictionary containing the market values for the last n days.
        """
        if days > 360:
            raise ValueError("The maximum number of days is 360.")

        endpoint = f"/leagues/{league_id}/players/{player_id}/stats"
        response = self._do_get(endpoint, True)
        data = response.json()

        # Extract the marketValues list from the data
        market_values_list = data["marketValues"]

        # Sort the marketValues list by date in descending order
        sorted_data = sorted(
            market_values_list,
            key=lambda x: datetime.fromisoformat(x["d"].replace("Z", "")),
            reverse=True,
        )

        # Get the market values for the last n days
        last_n_days_market_values = [x["m"] for x in sorted_data[:days]]
        last_n_days_market_values.reverse()
        return last_n_days_market_values

    def top_25_players(self) -> [Player]:
        r = self._do_get("/competition/best?position=0")

        if r.status_code == 200:
            return [Player(v) for v in r.json()["p"]]
        else:
            raise KickbaseException()

    def line_up(self, league: Union[str, LeagueData]) -> LineUp:
        league_id = self._get_league_id(league)

        r = self._do_get("/leagues/{}/lineup".format(league_id), True)

        if r.status_code == 200:
            return LineUp(r.json())
        else:
            raise KickbaseException()

    def set_line_up(self, line_up: LineUp, league: Union[str, LeagueData]) -> LineUp:
        league_id = self._get_league_id(league)

        if not self._is_token_valid:
            self.login(self._username, self._password)

        data = {"type": line_up.type, "players": line_up.players}

        r = self._do_post("/leagues/{}/lineup".format(league_id), data, True)

        if r.status_code == 200:
            return LineUp(r.json())
        else:
            raise KickbaseException()

    def market(self, league: Union[str, LeagueData]) -> Market:
        league_id = self._get_league_id(league)

        r = self._do_get("/leagues/{}/market".format(league_id), True)

        if r.status_code == 200:
            return Market(r.json())
        else:
            raise KickbaseException()

    def add_to_market(
        self,
        price: int,
        player: Union[str, Player, MarketPlayer],
        league: Union[str, LeagueData],
    ):
        player_id = self._get_player_id(player)
        league_id = self._get_league_id(league)

        data = {"playerId": player_id, "price": price}

        r = self._do_post("/leagues/{}/market".format(league_id), data, True)

        if r.status_code == 200:
            return
        else:
            raise KickbaseException()

    def remove_from_market(
        self, player: Union[str, Player, MarketPlayer], league: Union[str, LeagueData]
    ):
        player_id = self._get_player_id(player)
        league_id = self._get_league_id(league)

        r = self._do_delete("/leagues/{}/market/{}".format(league_id, player_id), True)

        if r.status_code == 200:
            return
        else:
            raise KickbaseException()

    def update_price(
        self,
        price: int,
        player: Union[str, Player, MarketPlayer],
        league: Union[str, LeagueData],
    ):
        player_id = self._get_player_id(player)
        league_id = self._get_league_id(league)

        data = {"price": price}

        r = self._do_put(
            "/leagues/{}/market/{}".format(league_id, player_id), data, True
        )

        if r.status_code == 200:
            return
        else:
            raise KickbaseException()

    def make_offer(
        self,
        price: int,
        player: Union[str, Player, MarketPlayer],
        league: Union[str, LeagueData],
    ):
        player_id = self._get_player_id(player)
        league_id = self._get_league_id(league)

        data = {"price": price}

        r = self._do_post(
            "/leagues/{}/market/{}/offers".format(league_id, player_id), data, True
        )

        if r.status_code == 200:
            return
        else:
            raise KickbaseException()

    def remove_offer(
        self,
        offer_id: str,
        player: Union[str, Player, MarketPlayer],
        league: Union[str, LeagueData],
    ):
        player_id = self._get_player_id(player)
        league_id = self._get_league_id(league)

        r = self._do_delete(
            "/leagues/{}/market/{}/offers/{}".format(league_id, player_id, offer_id),
            True,
        )

        if r.status_code == 200:
            return
        else:
            raise KickbaseException()

    def accept_offer(
        self,
        offer_id: str,
        player: Union[str, Player, MarketPlayer],
        league: Union[str, LeagueData],
    ):
        player_id = self._get_player_id(player)
        league_id = self._get_league_id(league)

        r = self._do_post(
            "/leagues/{}/market/{}/offers/{}/accept".format(
                league_id, player_id, offer_id
            ),
            {},
            True,
        )

        if r.status_code == 200:
            return
        else:
            raise KickbaseException()

    def decline_offer(
        self,
        offer_id: str,
        player: Union[str, Player, MarketPlayer],
        league: Union[str, LeagueData],
    ):
        player_id = self._get_player_id(player)
        league_id = self._get_league_id(league)

        r = self._do_post(
            "/leagues/{}/market/{}/offers/{}/decline".format(
                league_id, player_id, offer_id
            ),
            {},
            True,
        )

        if r.status_code == 200:
            return
        else:
            raise KickbaseException()

    def chat_token(self) -> str:
        if not self._is_token_valid():
            self.login(self._username, self._password)

        r = self._do_post("/user/refreshchattoken", {}, True)

        if r.status_code == 200:
            j = r.json()
            return j["token"]
        else:
            raise KickbaseException()

    def exchange_custom_token(self, chat_token: str):
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        data = {"returnSecureToken": True, "token": chat_token}

        r = requests.post(
            self._url_for_google_identity_toolkit(
                "/v3/relyingparty/verifyCustomToken?key={}".format(
                    self.google_identity_toolkit_api_key
                )
            ),
            data=json.dumps(data),
            headers=headers,
        )

        if r.status_code == 200:
            j = r.json()
            self.firebase_token = j["idToken"]
            self.firebase_token_expire = datetime.now(timezone.utc) + timedelta(
                seconds=int(j["expiresIn"])
            )
        else:
            raise KickbaseException(
                "There was an error exchanging custom token for firebase token"
            )

    def _update_firebase_token(self):
        token = self.chat_token()
        self.exchange_custom_token(token)

    def chat_messages(
        self,
        league: Union[str, LeagueData],
        page_size: int = 30,
        next_page_token: str = None,
    ) -> ([ChatItem], str):
        if self.google_identity_toolkit_api_key is None:
            return []

        league_id = self._get_league_id(league)

        if not self._is_token_valid():
            self.login(self._username, self._password)

        if not self._is_firebase_token_valid():
            self._update_firebase_token()

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "Bearer {}".format(self.firebase_token),
        }
        if next_page_token is None:
            r = requests.get(
                self._url_for_firestore(
                    "/chat/{}/messages?pageSize={}".format(league_id, page_size)
                ),
                headers=headers,
            )
        else:
            r = requests.get(
                self._url_for_firestore(
                    "/chat/{}/messages?pageSize={}&pageToken={}".format(
                        league_id, page_size, next_page_token
                    )
                ),
                headers=headers,
            )

        if r.status_code == 200:
            j = r.json()
            docs = j["documents"]
            npt = None
            if "nextPageToken" in j:
                npt = j["nextPageToken"]
            return [ChatItem(d) for d in docs], npt
        else:
            raise KickbaseException()

    def post_chat_message(self, message: str, league: Union[str, LeagueData]):
        if self.google_identity_toolkit_api_key is None:
            return

        league_id = self._get_league_id(league)

        if not self._is_token_valid():
            self.login(self._username, self._password)

        if not self._is_firebase_token_valid():
            self._update_firebase_token()

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "Bearer {}".format(self.firebase_token),
        }

        data = {
            "fields": {
                "userId": {"stringValue": self.user.id},
                "message": {"stringValue": message},
                "leagueId": {"stringValue": league_id},
                "date": {"stringValue": date_to_string(datetime.utcnow())},
                "username": {"stringValue": self.user.name},
                "seenBy": {"arrayValue": {"values": [{"stringValue": self.user.id}]}},
            }
        }

        r = requests.post(
            self._url_for_firestore("/chat/{}/messages".format(league_id)),
            data=json.dumps(data),
            headers=headers,
        )

        if r.status_code == 200:
            return
        else:
            raise KickbaseException()

    def _get_league_id(self, league: any):
        if isinstance(league, str):
            return league
        if isinstance(league, LeagueData):
            return league.id
        raise KickbaseException("league must be either type of str or LeagueData")

    def _get_player_id(self, player: any):
        if isinstance(player, str):
            return player
        if isinstance(player, Player):
            return player.id
        if isinstance(player, MarketPlayer):
            return player.id
        raise KickbaseException(
            "player must be either type of str, Player or MarketPlayer"
        )

    def _get_user_id(self, user: any):
        if isinstance(user, str):
            return user
        if isinstance(user, User):
            return user.id
        raise KickbaseException("user must be either type of str or User")

    def _get_feed_item_id(self, feed_item: any):
        if isinstance(feed_item, str):
            return feed_item
        if isinstance(feed_item, FeedItem):
            return feed_item.id
        raise KickbaseException("feed_item must be either type of str or FeedItem")

    def _url_for_firestore(self, document_endpoint: str):
        return (
            "https://firestore.googleapis.com/v1/projects/{}/databases/(default)/documents".format(
                self.firestore_project
            )
            + document_endpoint
        )

    def _url_for_google_identity_toolkit(self, endpoint: str):
        return "https://www.googleapis.com/identitytoolkit" + endpoint

    def _url_for_endpoint(self, endpoint: str):
        return self.base_url + endpoint

    def _auth_cookie(self):
        return "kkstrauth={}".format(self.token)

    def _do_get(self, endpoint: str, authenticated: bool = False, retries=3):
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if authenticated:
            headers["Cookie"] = self._auth_cookie()

        for i in range(retries):
            try:
                r = self.session.get(self._url_for_endpoint(endpoint), headers=headers)
                r.raise_for_status()
                return r
            except requests.RequestException as e:
                if i < retries - 1:
                    time.sleep(0.5)
                    continue
                else:
                    raise

    def _do_post(self, endpoint: str, data: dict, authenticated: bool = False):
        if authenticated and not self._is_token_valid():
            self.login(self._username, self._password)

        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if authenticated:
            headers["Cookie"] = self._auth_cookie()

        return requests.post(
            self._url_for_endpoint(endpoint), data=json.dumps(data), headers=headers
        )

    def _do_put(self, endpoint: str, data: dict, authenticated: bool = False):
        if authenticated and not self._is_token_valid():
            self.login(self._username, self._password)

        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if authenticated:
            headers["Cookie"] = self._auth_cookie()

        return requests.put(
            self._url_for_endpoint(endpoint), data=json.dumps(data), headers=headers
        )

    def _do_delete(self, endpoint: str, authenticated: bool = False):
        if authenticated and not self._is_token_valid():
            self.login(self._username, self._password)

        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if authenticated:
            headers["Cookie"] = self._auth_cookie()

        return requests.delete(self._url_for_endpoint(endpoint), headers=headers)

    def fetch_players_in_range(self, start, end):
        url = f"/competition/search?start={start}&end={end}"
        r = self._do_get(url, True)
        if r.status_code == 200:
            return [Player(v) for v in r.json()["p"]]
        else:
            raise KickbaseException()

    def fetch_user_id(self, player, league_id):
        url = f"/leagues/{league_id}/players/{player.id}"
        r = self._do_get(url, True)
        if r.status_code == 200:
            user_id = r.json().get("userId")
            if user_id is not None:
                player.user_id = user_id
        return player

    def get_all_players(self, league_id) -> [Player]:
        players = []
        ranges = [(i, i + 24) for i in range(0, 600, 25)]

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(self.fetch_players_in_range, *r) for r in ranges]
            for future in as_completed(futures):
                try:
                    players.extend(future.result())
                except Exception as e:
                    print(f"Error fetching players in range: {e}")

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = [
                executor.submit(self.fetch_user_id, p, league_id) for p in players
            ]
            for i, future in enumerate(as_completed(futures)):
                try:
                    players[i] = future.result()
                except Exception as e:
                    print(f"Error fetching user id for player: {e}")

        return players

    def leagueTable(self, matchDay=0):
        url = f"/competition/table?matchDay={matchDay}"
        r = self._do_get(url, True)
        if r.status_code == 200:
            data = r.json().get("t")
            return data
        else:
            raise KickbaseException()

    def matches(self, matchDay=0):
        url = f"/competition/matches?matchDay={matchDay}"
        r = self._do_get(url, True)
        if r.status_code == 200:
            data = r.json().get("m")
            return data
        else:
            raise KickbaseException()

    def get_next_games(self, team_id, next_n_games=5):
        # get current md
        url = f"/competition/matches?matchDay="
        r = self._do_get(url, True)
        if r.status_code == 200:
            cmd = r.json().get("cmd")

        # get next n mds
        mds = []
        for i in range(cmd, cmd + 5):
            url = f"/competition/matches?matchDay={i}"
            r = self._do_get(url, True)
            if r.status_code == 200:
                mds.append(r.json().get("m"))
        ngs = []
        for md in mds:
            for game in md:
                if game.get("t1i") == str(team_id):
                    ngs.append(game.get("t2i"))
                if game.get("t2i") == str(team_id):
                    ngs.append(game.get("t1i"))

        return ngs
