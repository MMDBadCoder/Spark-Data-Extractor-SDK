"""Fresh Connect sessions; no implicit getOrCreate/session reuse."""
import logging

LOG = logging.getLogger('data_extractor')


def create_session(config, name):
    from pyspark.sql import SparkSession
    # Assign the returned handle before performing optional server naming RPCs,
    # so a timeout during naming can still attempt release of that session.
    return SparkSession.builder.remote(config.connect_url).create()


def label(session, name):
    try:
        if hasattr(session, 'addTag'):
            session.addTag(name)
        if hasattr(session, 'conf'):
            session.conf.set('spark.app.name', name)
    except Exception:
        LOG.warning('event=session_naming_unsupported label=%s', name, exc_info=True)


def ready(session):
    # An observable server action, not a promise that executors are reserved.
    session.range(1).count()


def cancel(session):
    if session is not None:
        try:
            session.interruptAll()
        except Exception:
            LOG.exception('event=cancellation_failed remote_cleanup=unknown')


def release(session):
    if session is not None:
        try:
            session.stop()
            LOG.info('event=session_stop_returned remote_cleanup=unconfirmed')
        except Exception:
            LOG.exception('event=session_release_failed remote_cleanup=unknown')
