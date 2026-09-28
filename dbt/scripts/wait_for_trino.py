#!/usr/bin/env python3
"""Wake the Trino cluster and block until it can actually serve a query.

The production cluster runs on Railway with app sleeping enabled, so it may be
suspended when a scheduled dbt run fires. Railway wakes a sleeping service on
an inbound HTTP request, but the request that does the waking is not served --
so dbt's own first query would fail, and dbt-trino does not retry a failed
connection. Something has to knock first and then wait. That is this.

Two phases, because they answer different questions:

  1. IS IT AWAKE AND FINISHED STARTING? Polls `/v1/info`, which reports
     `starting` and needs no catalog. Any HTTP response at all proves the
     service is awake -- a 401 counts, since being told to authenticate means
     something is listening. 502/503/504 are Railway or Trino saying "still
     coming up", and a connection error means still asleep.

     A JVM cold start is slow; the default budget is generous.

  2. CAN IT RUN A QUERY? `SELECT 1` over the real credentials. Trino accepts
     connections before it will accept queries, and `/v1/info` may itself be
     behind auth on a secured cluster -- in which case phase 1 never gets to
     read `starting` and can only confirm "awake". So:

       * if phase 1 read `starting == false`, Trino has declared itself ready
         and this is a single confirming attempt: a failure here is a real
         credential, catalog or network problem and should surface immediately
         rather than being buried under retries.
       * if phase 1 could not read `starting`, this retries for whatever budget
         is left, because "awake" was all phase 1 could establish.

Configuration is the same environment the dbt profile reads (see
profiles.yml), so there is nothing extra to set in a deployment:

  TRINO_HOST          required
  TRINO_PORT          default 443
  TRINO_HTTP_SCHEME   default https  (set http for the local docker stack)
  TRINO_USER          default mobile-dbt
  TRINO_PASSWORD      HTTP Basic password; required over https
  MOBILE_CATALOG      default mobile

  TRINO_SSL_CERT      path to the cert to verify against; required over https

  TRINO_WAIT_TIMEOUT   total seconds for both phases, default 300
  TRINO_WAIT_INTERVAL  seconds between polls, default 5
"""

import base64
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request

# Railway returns these while a sleeping service is being woken, and Trino
# returns 503 for a coordinator that is up but still starting. Neither means
# "broken"; both mean "ask again".
WAKING_STATUSES = {502, 503, 504}


def env(name, default=None):
    value = os.environ.get(name, default)
    if value is None:
        sys.exit(f"wait_for_trino: {name} is not set")
    return value


def log(message):
    # Unbuffered so the ordering survives a container's log pipeline.
    print(f"wait_for_trino: {message}", flush=True)


def tls_verification():
    """How to verify the coordinator's certificate.

    Returns what `requests` wants for `verify`: a path to verify against, or
    False for no verification. Kept identical in meaning to the dbt profile's
    `cert`, so the probe and the run cannot disagree about whether the
    connection is checked.
    """
    # Required over https for the same reason the profile requires it: the
    # cluster is self-signed, so there is nothing in the system store that can
    # validate it, and this method puts the password on every request.
    if os.environ.get("TRINO_HTTP_SCHEME", "https") != "https":
        return True
    return env("TRINO_SSL_CERT")


def ssl_context(verify):
    """The same decision, as an SSLContext for urllib in phase 1."""
    if verify is True:
        return ssl.create_default_context()
    return ssl.create_default_context(cafile=verify)


def basic_auth_password():
    """The password, required over https.

    Enforced here rather than left to dbt. `env_var('TRINO_PASSWORD')` in the
    profile does NOT abort a run when the variable is missing -- dbt carries on
    and attempts the connection, so a deployment that forgot it fails later
    with an authentication error, which reads like a wrong password rather than
    an absent one. This runs before dbt and says which it is.
    """
    if os.environ.get("TRINO_HTTP_SCHEME", "https") != "https":
        return None
    return env("TRINO_PASSWORD")


def basic_auth_header(password):
    """`Authorization` for phase 1, so it can read `starting` on a secured
    cluster instead of stopping at a 401."""
    if not password:
        return {}
    user = os.environ.get("TRINO_USER", "mobile-dbt")
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def probe_info(url, timeout, headers, context):
    """One poll of /v1/info.

    Returns (awake, starting) where `starting` is None when it could not be
    read -- either because the endpoint is behind auth or because the body was
    not the JSON we expected.
    """
    request = urllib.request.Request(url, method="GET", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            try:
                return True, bool(json.load(response).get("starting"))
            except (ValueError, AttributeError):
                # Awake, but the body is not what we expected. Do not guess.
                return True, None
    except urllib.error.HTTPError as err:
        if err.code in WAKING_STATUSES:
            return False, None
        # 401/403/404/anything else: something is listening and answering.
        log(f"HTTP {err.code} from /v1/info -- awake, but `starting` is not readable")
        return True, None
    except (urllib.error.URLError, TimeoutError, OSError) as err:
        reason = getattr(err, "reason", err)
        log(f"not reachable yet ({reason})")
        return False, None


def wait_for_http(url, deadline, interval, request_timeout, headers, context):
    """Phase 1. Returns True if `starting == false` was actually observed."""
    log(f"waking {url}")
    while True:
        awake, starting = probe_info(url, request_timeout, headers, context)
        if awake and starting is False:
            log("coordinator reports starting=false")
            return True
        if awake and starting is None:
            log("awake but starting is unreadable; deferring to the query check")
            return False
        if awake:
            log("awake, still starting")
        if time.monotonic() >= deadline:
            sys.exit("wait_for_trino: timed out waiting for the coordinator")
        time.sleep(interval)


def try_query(host, port, scheme, verify, password):
    """`SELECT 1`. Raises on failure; the caller decides whether to retry."""
    # dbt-trino's own client, so this adds no dependency and speaks the same
    # protocol the real run will.
    import trino

    user = env("TRINO_USER", "mobile-dbt")
    auth = trino.auth.BasicAuthentication(user, password) if password else None

    connection = trino.dbapi.connect(
        host=host,
        port=int(port),
        user=user,
        catalog=env("MOBILE_CATALOG", "mobile"),
        http_scheme=scheme,
        auth=auth,
        verify=verify,
    )
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchall()
    finally:
        connection.close()


def wait_for_query(host, port, scheme, verify, password, deadline, interval, retry):
    log("checking that it will serve a query")
    while True:
        try:
            try_query(host, port, scheme, verify, password)
            log("SELECT 1 succeeded; Trino is ready")
            return
        except Exception as err:  # noqa: BLE001 - any failure is a failure to serve
            if not retry:
                # Phase 1 already saw starting=false, so this is not a cluster
                # that needs more time.
                sys.exit(f"wait_for_trino: Trino is up but the query failed: {err}")
            if time.monotonic() >= deadline:
                sys.exit(f"wait_for_trino: timed out; last query error: {err}")
            log(f"query not served yet ({err})")
            time.sleep(interval)


def main():
    host = env("TRINO_HOST")
    port = env("TRINO_PORT", "443")
    scheme = env("TRINO_HTTP_SCHEME", "https")
    interval = float(env("TRINO_WAIT_INTERVAL", "5"))
    timeout = float(env("TRINO_WAIT_TIMEOUT", "300"))

    deadline = time.monotonic() + timeout
    # Per-request timeout: long enough that a slow wake is not mistaken for an
    # unreachable host, short enough to keep polling inside the budget.
    request_timeout = min(interval * 2, 30)

    # Both resolved before any waiting, so a missing one fails in a second
    # rather than after the full wake budget.
    verify = tls_verification()
    password = basic_auth_password()

    saw_ready = wait_for_http(
        f"{scheme}://{host}:{port}/v1/info",
        deadline,
        interval,
        request_timeout,
        basic_auth_header(password),
        ssl_context(verify) if scheme == "https" else None,
    )
    wait_for_query(
        host, port, scheme, verify, password, deadline, interval, retry=not saw_ready
    )


if __name__ == "__main__":
    main()
