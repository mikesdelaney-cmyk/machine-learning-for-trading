	 uv run python - <<'PY'
import asyncio
import os
from datetime import UTC, datetime, timedelta

from dotenv import load_dotenv
from ml4t.live.feeds.alpaca_feed import AlpacaDataFeed

load_dotenv(".env")

DURATION_SECONDS = 180
BAR_INTERVAL = timedelta(minutes=1)

async def main():
    feed = AlpacaDataFeed(
        api_key=os.environ["ALPACA_API_KEY"],
        secret_key=os.environ["ALPACA_SECRET_KEY"],
        symbols=["SPY", "QQQ", "IWM"],
        data_type="bars",
        feed="iex",
        experimental=True,
    )

    count = 0
    max_provider_age = 0.0
    max_post_close_latency = 0.0
    max_queue_age = 0.0
    max_total_age = 0.0

    await feed.start()

    print("Direct Alpaca feed timing diagnostic started")
    print("Broker: NOT CONNECTED")
    print("Orders: IMPOSSIBLE")
    print("Duration:", DURATION_SECONDS, "seconds")
    print()

    try:
        async with asyncio.timeout(DURATION_SECONDS):
            async for event in feed:
                processing_time = datetime.now(UTC)

                expected_close = event.event_time + BAR_INTERVAL
                provider_age = (
                    event.receipt_time - event.event_time
                ).total_seconds()

                post_close_latency = (
                    event.receipt_time - expected_close
                ).total_seconds()

                queue_age = (
                    processing_time - event.receipt_time
                ).total_seconds()

                total_age = (
                    processing_time - event.event_time
                ).total_seconds()

                count += 1

                max_provider_age = max(max_provider_age, provider_age)
                max_post_close_latency = max(
                    max_post_close_latency, post_close_latency
                )
                max_queue_age = max(max_queue_age, queue_age)
                max_total_age = max(max_total_age, total_age)

                print(
                    f"{count:4d} "
                    f"{event.asset:4s} "
                    f"event={event.event_time.isoformat()} "
                    f"receipt={event.receipt_time.isoformat()} "
                    f"provider_age={provider_age:8.3f}s "
                    f"post_close={post_close_latency:8.3f}s "
                    f"queue_age={queue_age:8.3f}s "
                    f"total_age={total_age:8.3f}s"
                )

                if post_close_latency > 5:
                    print()
                    print("*** STALE EVENT REPRODUCED ***")
                    print("provider_age:", f"{provider_age:.3f}s")
                    print("post_close_latency:", f"{post_close_latency:.3f}s")
                    print("queue_age:", f"{queue_age:.3f}s")
                    print("total_age:", f"{total_age:.3f}s")
                    break

    except TimeoutError:
        print()
        print("Diagnostic duration reached normally")

    finally:
        feed.stop()

    print()
    print("=== SUMMARY ===")
    print("bars observed:", count)
    print("max provider age:", f"{max_provider_age:.3f}s")
    print("max post-close latency:", f"{max_post_close_latency:.3f}s")
    print("max queue age:", f"{max_queue_age:.3f}s")
    print("max total age:", f"{max_total_age:.3f}s")

asyncio.run(main())
PY

	 history | tail -30
	 history | tail -80
	 history | tail -140
