"""Module with tools for running DynamoDBLocal for testing

This package requires a ``java`` command on the command line.
"""

from .version import __version__

from contextlib import contextmanager, ExitStack
import json
import os.path
import socket
import subprocess as subp
from typing import Any, Callable, Iterable, Optional
from unittest.mock import patch

import logging
_log = logging.getLogger(__name__)

@contextmanager
def in_subprocess(
    dynamodblocal_path: str,
    *,
    port_range: Optional[Iterable[int]] = None,
):
    """Provide an in-memory, local DynamoDB service on an unused port
    
    :param dynamodblocal_path:
        Path to the unpacked DynamoDBLocal software; current site for
        obtaining this software is https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
    :keyword port_range:
        An *iterable* of TCP port numbers to try, where the first one that
        refuses a TCP connection is selected; default is ``range(8100, 8500)``
    :keyword on_server_missing:
        If given, called back when *dynamodblocal_path* is ``None`` and a
        DynamoDB operation is attempted; this might raise an exception for the
        test case to be skipped
    
    The port number (an :class:`int`) is yielded as the context value.
    """
    # Find an available TCP port as *port*
    port_range = port_range or range(8100, 8500)
    for port in port_range:
        try:
            socket.create_connection(('localhost', port), 0.001).close()
        except ConnectionRefusedError:
            break
        port = None
    
    if port is None:
        raise Exception(f"No sockets available in {port_range}")
    
    # Start the dynamodb_local server on *port*
    _log.debug('Opening DynamoDBLocal on port %d', port)
    db_server = subp.Popen(
        ['java', '-Djava.library.path=./DynamoDBLocal_lib', '-jar', 'DynamoDBLocal.jar', '-inMemory', '-port', str(port)],
        cwd=dynamodblocal_path,
        stdout=subp.PIPE,
    )
    _log.debug('DynamoDBLocal server (pid %d) on port %d', db_server.pid, port)
    
    try:
        returncode = db_server.wait(timeout=0.1)
        raise Exception(f"DynamoDBLocal returned code {returncode}")
    except subp.TimeoutExpired:
        pass # This is what we want to see
    
    endpoint_kwargs = dict(
        endpoint_url=f"http://localhost:{port}",
        use_ssl=False,
    )
        
    try:
        yield port
    finally:
        _log.debug('Terminating DynamoDBLocal server (pid %d)', db_server.pid)
        db_server.terminate()
        try:
            returncode = db_server.wait()
            _log.debug('DynamoDBLocal (pid %d) server has exited with code %d', db_server.pid, returncode)
        except KeyboardInterrupt:
            _log.warning('Killing DynamoDBLocal server (pid %d), not waiting', db_server.pid)
            db_server.kill()
            raise

@contextmanager
def patched_into_boto3(
    dynamodblocal_path: Optional[str],
    *,
    port_range: Optional[Iterable[int]] = None,
    on_server_missing: Optional[Callable[[], Any]] = None,
):
    """Provide an in-memory, local DynamoDB service on an unused port
    
    :param dynamodblocal_path:
        Path to the unpacked DynamoDBLocal software; current site for
        obtaining this software is https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
    :keyword port_range:
        An *iterable* of TCP port numbers to try, where the first one that
        refuses a TCP connection is selected; default is ``range(8100, 8500)``
    :keyword on_server_missing:
        If given, called back when *dynamodblocal_path* is ``None`` and access
        to the ``'dynamodb'`` service through :mod:`boto3` is attempted; this
        might, for example, raise an exception for the test case to be skipped
    
    The local service is patched into :mod:`boto3` via :mod:`boto3_mocking`
    while the returned context is active.  It is an error to *enter* the
    context (though not to create the context) before calling
    :func:`boto3_mocking.engage_patching`.
    """
    import boto3_mocking
    
    if not boto3_mocking.patching_engaged():
        raise Exception("boto3_mocking.engage_patching() MUST be called before entering this context")
    
    if dynamodblocal_path is None and on_server_missing is None:
        raise Exception("No DynamoDBLocal configured (see logged errors)")
    
    if dynamodblocal_path is not None:
        with in_subprocess(dynamodblocal_path, port_range=port_range) as port:
            endpoint_kwargs = dict(
                endpoint_url=f"http://localhost:{port}",
                use_ssl=False,
            )
            
            def mock_client(**kwargs):
                return boto3_mocking.clients.real(
                    'dynamodb',
                    **dict(kwargs, **endpoint_kwargs)
                )
            
            def mock_resource(**kwargs):
                return boto3_mocking.resources.real(
                    'dynamodb',
                    **dict(kwargs, **endpoint_kwargs)
                )
            
            with ExitStack() as db_ctx:
                boto3_mocking.enter_handlers(
                    db_ctx,
                    'dynamodb',
                    clients=mock_client,
                    resources=mock_resource,
                )
                yield
    
    else:
        def mock_handler(**kwargs):
            return on_server_missing()
        
        with ExitStack() as db_ctx:
            boto3_mocking.enter_handlers(
                db_ctx, 'dynamodb',
                clients=mock_handler,
                resources=mock_handler,
            )
            yield

class LocalTableBuilder:
    """Create DynamoDB tables according to the Serverless config in a local DynamoDB
    
    Table specifications are read via the ``serverless print`` command, so
    the ``serverless`` tool must be installed for this to function properly.
    The serverless config is only read when this object is created so this
    expensive operation can be amortized.
    """
    def __init__(self, serverless_config_path: str):
        super().__init__()
        self._serverless_config_path = serverless_config_path
        sls_dir, sls_config = os.path.split(serverless_config_path)
        sls_proj = json.loads(subp.check_output(
            [
                'serverless', 'print',
                '--format=json',
                '--config', sls_config,
            ],
            cwd=sls_dir,
        ))
        self._resources = sls_proj['resources']['Resources']
    
    @property
    def serverless_config_path(self):
        return self._serverless_config_path
    
    @property
    def tables(self):
        yield from (
            r
            for r in self._resources.values()
            if r['Type'] == 'AWS::DynamoDB::Table'
        )
    
    def recreate_through(self, dynamodb_client):
        """Create DynamoDB tables according to the Serverless config in a local DynamoDB
        
        This method asserts that the client it is given connects to some TCP
        port on localhost.  Use an appropriate system (like the context manager
        :func:`.patched_into_boto3`) to provide a DynamoDBLocal instance.
        
        Tables in the Serverless config that already exist are dropped and
        recreated.
        """
        ddb = dynamodb_client
        assert ddb.meta.endpoint_url.startswith('http://localhost:')
        existing_tables = ddb.list_tables()['TableNames']
        
        for t in self.tables:
            if t['Properties']['TableName'] in existing_tables:
                ddb.delete_table(TableName=t['Properties']['TableName'])
            ddb.create_table(**t['Properties'])

class LocalDbOps:
    """Support for operations on a DynamoDBLocal"""
    def __init__(self, *, serverless_config: Optional[str] = None):
        super().__init__()
        self._serverless_config = serverless_config
    
    @property
    def serverless_config(self):
        return self._serverless_config
    
    @property
    def table_builder(self):
        if not hasattr(self, '_table_builder'):
            self._table_builder = LocalTableBuilder(self.serverless_config)
        return self._table_builder
    
    def fresh_test_tables(self, dynamodb_resource, fixture_data: Optional[dict] = None):
        """Create or recreate tables and fill with the given data
        
        If *fixture_data* is not ``None``, it should be a :class:`dict` whose
        keys are the DynamoDB table names and values are the items to insert
        into those tables.  Item format follows the :mod:`boto3` *resource*
        usage rather than the *client* usage: item attribute types are inferred
        from the Python types rather than explicitly specified.
        """
        ddb = dynamodb_resource
        self.table_builder.recreate_through(ddb.meta.client)
        
        _log.info('%d tables to populate', len(fixture_data or ()))
        for table_name in (fixture_data or ()):
            table = ddb.Table(table_name)
            with table.batch_writer() as batch:
                for item in fixture_data[table_name]:
                    _log.debug('Adding item to table %s: %r', table_name, item)
                    batch.put_item(Item=item)
