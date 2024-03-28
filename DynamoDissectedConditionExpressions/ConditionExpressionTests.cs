using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDissectedConditionExpressions;

[Collection("dynamo")]
public class ConditionExpressionTests
{
    private readonly IAmazonDynamoDB dynamo = new AmazonDynamoDBClient(
        new BasicAWSCredentials("unused", "unused"),
        new AmazonDynamoDBConfig {ServiceURL = "http://localhost:8000"});

    public ConditionExpressionTests(DynamoFixture fixture)
    {
        fixture.KillContainerAfterTests = true; // Flip this if you want to inspect the database after the tests run
    }

    /// <summary>
    /// The item (1, 1) exists, so Dynamo will load that, check to see if it has "pk" (obviously it does) and fail.
    /// </summary>
    [Fact]
    public async Task Pk_not_exists()
    {
        var table = await this.CreateTable();

        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = table,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new("1"),
                ["sk"] = new("1")
            }
        });

        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = table,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new("1"),
                ["sk"] = new("2")
            },
            ConditionExpression = "attribute_not_exists(pk)"
            // This is fine because (1, 2) does not exist, remember that Dynamo will first select the item with key
            // (1, 2) and _then_ evaluate the condition. It can't select it here, so the condition defaults to true.
        });
        
        var invalidPut = async () => await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = table,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new("1"),
                ["sk"] = new("1")
            },
            ConditionExpression = "attribute_not_exists(pk)"
            // And this is not fine specifically because Dynamo will be able to select (1, 1) and evaluate that "pk"
            // does in fact exist.
        });

        await Assert.ThrowsAsync<ConditionalCheckFailedException>(invalidPut);
    }

    
    [Fact]
    public async Task Sk_not_exists()
    {
        var table = await this.CreateTable();

        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = table,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new("1"),
                ["sk"] = new("1")
            }
        });

        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = table,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new("2"),
                ["sk"] = new("1")
            },
            ConditionExpression = "attribute_not_exists(sk)"
            // This is once again fine - (2, 1) does not exist, so the condition defaults to true.
        });
        
        var invalidPut = async () => await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = table,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new("1"),
                ["sk"] = new("1")
            },
            ConditionExpression = "attribute_not_exists(sk)"
            // Because (1, 1) exists, this condition has to fail.
        });

        await Assert.ThrowsAsync<ConditionalCheckFailedException>(invalidPut);
    }

    [Fact]
    public async Task Pk_and_sk_not_exists()
    {
        var table = await this.CreateTable();

        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = table,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new("1"),
                ["sk"] = new("1")
            }
        });

        var invalidPut = async () => await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = table,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new("1"),
                ["sk"] = new("1")
            },
            ConditionExpression = "attribute_not_exists(pk) AND attribute_not_exists(sk)"
            // As the other tests prove, this condition is now a bit redundant - we know either one here would suffice.
            // Again, logically, Dynamo will load (1, 1) _because_ it exists, then the PK or SK check will fail.
        });

        await Assert.ThrowsAsync<ConditionalCheckFailedException>(invalidPut);
    }

    private async Task<string> CreateTable(string? name = null)
    {
        name ??= Guid.NewGuid().ToString("N");
        await this.dynamo.CreateTableAsync(new CreateTableRequest
        {
            TableName = name,
            KeySchema =
            [
                new KeySchemaElement("pk", KeyType.HASH),
                new KeySchemaElement("sk", KeyType.RANGE)
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition("pk", ScalarAttributeType.S),
                new AttributeDefinition("sk", ScalarAttributeType.S)
            ],
            BillingMode = BillingMode.PAY_PER_REQUEST
        });

        return name;
    }
}