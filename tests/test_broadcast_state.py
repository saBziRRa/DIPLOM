from btc_forecast.bot import userstate


def _redirect(tmp_path, monkeypatch):
    store = tmp_path / "vk_userstate.json"
    monkeypatch.setattr(userstate, "_store_path", lambda: store)


def test_last_broadcast_roundtrip(tmp_path, monkeypatch):
    _redirect(tmp_path, monkeypatch)
    assert userstate.get_last_broadcast() is None
    userstate.set_last_broadcast("1h|ts|1")
    assert userstate.get_last_broadcast() == "1h|ts|1"


def test_last_broadcast_does_not_drop_users(tmp_path, monkeypatch):
    _redirect(tmp_path, monkeypatch)
    userstate.subscribe(42)
    userstate.set_last_broadcast("6h|ts|-1")
    subs = {s["user_id"] for s in userstate.list_subscribers()}
    assert 42 in subs


def test_subscriber_thresholds(tmp_path, monkeypatch):
    _redirect(tmp_path, monkeypatch)
    userstate.subscribe(1)
    userstate.set_min_confidence(1, 0.8)
    userstate.subscribe(2)
    subs = {s["user_id"]: s["min_confidence"] for s in userstate.list_subscribers()}
    assert subs[1] == 0.8
    assert subs[2] == userstate.DEFAULT_MIN_CONFIDENCE


def test_unsubscribe_excludes_from_list(tmp_path, monkeypatch):
    _redirect(tmp_path, monkeypatch)
    userstate.subscribe(7)
    userstate.unsubscribe(7)
    assert all(s["user_id"] != 7 for s in userstate.list_subscribers())
