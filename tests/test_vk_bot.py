from btc_forecast.vkbot.app import parse_command


def test_parse_command_basic():
    assert parse_command("/summary") == ("summary", None)
    assert parse_command("forecast 6h") == ("forecast", "6h")
    assert parse_command("/chart@mybot 1h") == ("chart", "1h")


def test_parse_command_new_commands():
    assert parse_command("/subscribe") == ("subscribe", None)
    assert parse_command("/unsubscribe") == ("unsubscribe", None)
    assert parse_command("/settings 0.7") == ("settings", "0.7")
    assert parse_command("/help") == ("help", None)
    assert parse_command("/info") == ("info", None)


def test_parse_command_russian_aliases():
    assert parse_command("прогноз 1h") == ("forecast", "1h")
    assert parse_command("помощь") == ("help", None)
    assert parse_command("подписаться") == ("subscribe", None)
    assert parse_command("отписка") == ("unsubscribe", None)
    assert parse_command("настройки 0.7") == ("settings", "0.7")


def test_parse_command_unknown_or_empty():
    assert parse_command("") == (None, None)
    assert parse_command("  ") == (None, None)
    assert parse_command("hello bot") == (None, None)
