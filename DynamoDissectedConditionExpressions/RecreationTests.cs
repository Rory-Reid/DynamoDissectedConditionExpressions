using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace DynamoDissectedConditionExpressions;

/// <summary>
/// These tests hopefully illustrate that the <see cref="Recreation"/> simulates a conditional put with non-existence
/// checks, and then uses that simulation to prove that the following are all equivalent:
///
/// - attribute_not_exists(pk)
/// - attribute_not_exists(sk)
/// - attribute_not_exists(pk) AND attribute_not_exists(sk)
///
/// Most of these tests implement a proof just through lack of crashing. This isn't meant to be an exhaustive example or
/// test suite.
/// </summary>
public class RecreationTests
{
    private readonly Recreation dynamo = new();
    
    [Fact]
    public void Put_no_condition()
    {
        this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "value"
        });
    }

    [Fact]
    public void Replace_no_condition()
    {
        this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "value"
        });
        
        this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "replaced-value"
        });
    }
    
    [Fact]
    public void Put_if_irrelevant_attribute_not_exists()
    {
        this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "value"
        }, ["irrelevant"]);
    }
    
    [Fact]
    public void Replace_if_irrelevant_attribute_not_exists()
    {
        this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "value"
        }, ["irrelevant"]);

        this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "replaced-value"
        }, ["irrelevant"]);
    }

    [Fact]
    public void Pk_not_exists()
    {
        this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "value"
        }, ["pk"]);

        var violatingPut = () => this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "replaced-value"
        }, ["pk"]);
        
        var ex = Assert.Throws<ConditionalCheckFailedException>(violatingPut);
        Assert.Equal("failed attribute_not_exists(pk)", ex.Message);
    }
    
    [Fact]
    public void Sk_not_exists()
    {
        this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "value"
        }, ["sk"]);

        var violatingPut = () => this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "replaced-value"
        }, ["sk"]);
        
        var ex = Assert.Throws<ConditionalCheckFailedException>(violatingPut);
        Assert.Equal("failed attribute_not_exists(sk)", ex.Message);
    }
    
    [Fact]
    public void Pk_and_sk_not_exists()
    {
        this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "value"
        }, ["pk", "sk"]);

        var violatingPut = () => this.dynamo.PutItem(new Document
        {
            ["pk"] = "123",
            ["sk"] = "abc",
            ["attribute"] = "replaced-value"
        }, ["pk", "sk"]);
        
        var ex = Assert.Throws<ConditionalCheckFailedException>(violatingPut);
        Assert.Equal("failed attribute_not_exists(pk) AND attribute_not_exists(sk)", ex.Message);
    }
}