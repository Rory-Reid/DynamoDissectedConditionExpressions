# Sample: checking for (non)existence in DynamoDB

This repository was built to support [my blog post about a common misunderstanding in DynamoDB condition expressions, particularly when validating existence](https://rory.horse/posts/dynamo-dissected-condition-expression-existence-check/).

This repository is implemented as a test project which demonstrates two things:

1. A proof for my assertion that you can simplify your expressions to achieve the same result, using a containerised `dynamodb-local` instance to validate that.
2. A "plain old code" recreation of the logic that DynamoDB uses which hopefully illustrates why existence checks behave the way they do.

The only pre-requisite you should need to run these tests is Docker.