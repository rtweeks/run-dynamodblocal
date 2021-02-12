# Run-DynamoDBLocal

AWS makes [DynamoDBLocal][ddbl] -- a service that mirrors the API of DynamoDB and runs locally -- available to their customers for testing purposes.  This Python packages facilitates running that service as a subprocess of the testing harness.

## Install

```console
$ pip install 'run-dynamodblocal[boto3]'
```

Leave off the `[boto3]` if you don't intend to use this package's ability to patch `boto3`.

## Usage

### Quickstart

With thanks to [ncoghlan](https://stackoverflow.com/a/45809502/160072):

```python
import boto3_mocking
from contextlib import ExitStack
import os.path
import run_dynamodblocal
import unittest

# If patching isn't engaged when the run_dynamodblocal context is entered, an
# exception will be thrown
boto3_mocking.engage_patching()

class MyTest(unittest.TestCase):
    def setUp(self):
        with ExitStack() as resources:
            dynamodblocal_path = os.path.expanduser(
              # wherever you keep the unpacked service binaries
              '~/Downloads/dynamodb_local_latest'
            )
            resources.enter_context(
                run_dynamodblocal.rddbl.patched_into_boto3(dynamodblocal_path)
            )
            
            # Enter any other contexts for this test class...
            
            addCleanup(resources.pop_all().close)
    
    # Define test methods here
```

### Diving Deeper

This package provides two different context managers which run the DynamoDBLocal service -- one of which integrates with `boto3` to automatically redirect the `dynamodb` service to this DynamoDBLocal instance.  The DynamoDBLocal started will use the `-inMemory` flag, so it does not persist on disk after the test run is over and doesn't waste testing time committing information to disk.

| Context Manager                        | Description |
| :------------------------------------- | :------------- |
| `run_dynamodblocal.in_subprocess`      | The most fundamental; runs the server and returns the port number as the context value |
| `run_dynamodblocal.patched_into_boto3` | Runs the server and patches it into the `boto3` library |

It also provides the `run_dynamodblocal.LocalDbOps` class, which supports refreshing the database schema and populated data based on a Serverless configuration and some basic JSON-type data.

## Contributing

1. Fork it on GitHub (https://github.com/rtweeks/run-dynamodblocal)
1. Create your feature branch (`git checkout -b my-new-feature`)
1. Commit your changes (`git commit -am 'Add some feature'`)
1. Push to the branch (`git push origin my-new-feature`)
1. Create a new Pull Request (on [GitHub](https://github.com))



[ddbl]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
