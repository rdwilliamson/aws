Some of the tests require AWS credentials to be set, as they upload to Glacier.
If the following environment variables are not set, those tests will be skipped:

```
AWS_SECRET_KEY
AWS_ACCESS_KEY
GLACIER_VAULT
GLACIER_REGION
```

Use `go test -v` to see which tests are run and which are skipped.

The tests make the best effort to cleanup uploaded archives. If you set these
credentials correctly and run the tests, you will incur request and storage
pricing (http://aws.amazon.com/glacier/pricing/).
