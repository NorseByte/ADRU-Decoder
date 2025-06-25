import time
import sys


def run_statistic_generation():
    animation = [
        "📊 Generating stats...",
        "📊 Crunching numbers...    ",
        "📊 Modeling pigeons... 🐦📉",
        "📊 Consulting the oracle... 🔮",
        "📊 Almost there... 99.9%",
        "📊 Complete! 🎉"
    ]

    print("\n🔮 Welcome to the ADRU Statistic Engine\n")
    for _ in range(2):  # Run animation loop twice
        for frame in animation[:-1]:
            sys.stdout.write("\r" + frame)
            sys.stdout.flush()
            time.sleep(1)

    # Final frame
    sys.stdout.write("\r" + animation[-1] + "\n")
    sys.stdout.flush()
    print("\n✅ Statistic generation complete... just kidding, coming soon! 😄")

