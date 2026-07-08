"""Fake workload reporting metrics, transactions, and errors to RootTrace.

    ROOTTRACE_APM_TOKEN=rtc_... python3 examples/demo.py
"""

import http.client
import random
import time

import roottrace_apm as apm

apm.init(service="demo-app", service_version="0.1.0", interval_seconds=5)

orders = apm.counter("orders.processed")
queue = apm.gauge("queue.depth")
checkout = apm.timer("checkout.duration", tags={"endpoint": "/checkout"})


@apm.timed("demo.work.duration")
def work():
    time.sleep(random.uniform(0.01, 0.1))


def outbound_call():
    # goes through the patched http.client, so it records
    # http.client.* metrics and an http span on the open transaction
    conn = http.client.HTTPConnection("example.com", 80, timeout=5)
    try:
        conn.request("GET", "/")
        conn.getresponse().read()
    finally:
        conn.close()


def process_order():
    with apm.transaction("POST /checkout"):
        with apm.span("SELECT orders", type="db", subtype="postgresql"):
            work()
        with apm.span("cart lookup", type="cache", subtype="redis"):
            time.sleep(random.uniform(0.001, 0.01))
        try:
            outbound_call()
        except OSError as exc:  # offline demo hosts still exercise the rest
            apm.capture_exception(exc)


print("reporting for ~30s, flushing every 5s; ctrl-c to stop early")
for _ in range(60):
    with checkout:
        process_order()
    orders.add()
    queue.set(random.randint(0, 25))
    time.sleep(0.4)

apm.shutdown()
