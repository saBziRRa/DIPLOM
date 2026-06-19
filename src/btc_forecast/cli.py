"""CLI entry points."""

from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def ingest_cmd() -> None:
    from btc_forecast.pipeline.ingest import run_ingest

    parser = argparse.ArgumentParser(description="Download and cache BTC data")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(sys.argv[1:] or [])
    run_ingest(force=args.force)


def train_cmd() -> None:
    from btc_forecast.pipeline.features import run_features
    from btc_forecast.pipeline.train import run_train

    parser = argparse.ArgumentParser(description="Build features and train models")
    parser.add_argument("--skip-tune", action="store_true")
    parser.add_argument("--skip-features", action="store_true")
    args = parser.parse_args(sys.argv[1:] or [])
    if not args.skip_features:
        run_features()
    run_train(tune=not args.skip_tune)


def bot_cmd() -> None:
    from btc_forecast.bot.app import main

    main()


def vk_bot_cmd() -> None:
    from btc_forecast.vkbot.app import main

    main()


def debug_cmd() -> None:
    from btc_forecast.debug.server import main

    main(mode="debug")


def dashboard_cmd() -> None:
    from btc_forecast.debug.server import main

    main(mode="dashboard")


def benchmark_cmd() -> None:
    from btc_forecast.pipeline.benchmark import run_benchmark

    parser = argparse.ArgumentParser(description="Run bundle benchmark and persist metrics")
    parser.add_argument("--name", default="")
    parser.add_argument("--description", default="")
    args = parser.parse_args(sys.argv[1:] or [])
    result = run_benchmark(name=args.name, description=args.description)
    print(result.to_string(index=False))


def predict_cmd() -> None:
    from btc_forecast.bot import formatters  # noqa: PLC0415
    from btc_forecast.pipeline.predict import run_predict

    result = run_predict()
    print(formatters.format_summary(result))
    print()
    print(formatters.format_forecast(result))


if __name__ == "__main__":
    ingest_cmd()
