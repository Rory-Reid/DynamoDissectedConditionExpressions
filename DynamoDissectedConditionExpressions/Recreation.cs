using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace DynamoDissectedConditionExpressions;

/// <summary>
/// This class recreates the scenario where we put an item with an optional expression checking for attribute
/// nonexistence. The model is very simple and can only be used to recreate an unbounded chain of non-existence checks
/// ANDed together, e.g. "attribute_not_exists(a) AND attribute_not_exists(b) AND attribute_not_exists(c)".
///
/// More realistically, this can be used to demonstrate why there is no practical difference between the following:
/// - attribute_not_exists(pk)
/// - attribute_not_exists(sk)
/// - attribute_not_exists(pk) AND attribute_not_exists(sk)
///
/// This is because dynamo _identifies the item first_ before checking the condition. You can't identify something that
/// does not have an existent key part.
///
/// What's really important to underline here is that existence checks only check for attribute names, not values. Key
/// attributes are a bit unique here because value is implicitly proven through nonexistence.
///
/// The equivalent schema in dynamodb would be
/// { Type: HASH, Name: pk, AttributeType: S }
/// { Type: RANGE, Name: sk, AttributeType: S }
/// </summary>
public class Recreation
{
    private readonly Dictionary<Key, Document> items = new();

    public void PutItem(Document item, string[]? attributeNotExists = null)
    {
        var pk = item["pk"].AsString();
        var sk = item["sk"].AsString();

        var key = new Key(pk, sk);
        var conditionFailed = false;
        if (this.items.TryGetValue(key, out var existing))
        {
            if (attributeNotExists != null)
            {
                foreach (var attribute in attributeNotExists)
                {
                    if (existing.ContainsKey(attribute))
                    {
                        conditionFailed = true;
                    }
                }
            }
        }

        if (conditionFailed)
        {
            var nonExistenceExpressions = attributeNotExists!.Select(attribute => $"attribute_not_exists({attribute})");
            throw new ConditionalCheckFailedException($"failed {string.Join(" AND ", nonExistenceExpressions)}");
        }
        
        this.items[key] = item;
    }
}

public record Key(string PartitionKey, string SortKey);