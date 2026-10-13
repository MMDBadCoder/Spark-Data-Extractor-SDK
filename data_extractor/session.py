"""Create and release fresh Spark Connect sessions using the Spark 3.4 API."""
import logging

LOG = logging.getLogger('data_extractor')


def create_session(config, client_execution_id=None):
    from pyspark.sql import SparkSession
    # Spark 3.4.4 Connect builder supports remote(url).getOrCreate(). It does
    # not expose appName here; execution_id is a client-side correlation ID.
    if client_execution_id:
        LOG.info('event=session_create client_correlation_id=%s', client_execution_id)
    return SparkSession.builder.remote(config.connect_url).getOrCreate()


def ready(session):
    # An observable server action, not a promise that executors are reserved.
    session.range(1).count()


def cancel(session):
    if session is not None:
        interrupt = getattr(session, 'interruptAll', None)
        if interrupt is None:
            LOG.warning('event=cancellation_unsupported remote_cleanup=unknown')
            return
        try:
            interrupt()
        except Exception as exc:
            LOG.warning('event=cancellation_failed error_type=%s remote_cleanup=unknown', type(exc).__name__)


def release(session):
    if session is not None:
        try:
            session.stop()
            LOG.info('event=session_stop_returned remote_cleanup=unconfirmed')
        except Exception:
            LOG.exception('event=session_release_failed remote_cleanup=unknown')
