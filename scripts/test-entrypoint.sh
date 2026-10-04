#!/bin/sh
set -eu

# Защита охватывает миграции и seed, которые выполняются раньше pytest.
python -m signalwatch.test_safety
exec "$@"
