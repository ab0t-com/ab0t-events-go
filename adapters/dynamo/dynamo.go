// Package dynamo provides DynamoDB adapters for ab0t-events-go's
// SubscriptionStore and DeliveryLog ports, using aws-sdk-go-v2.
//
// # Tenancy
//
// Both tables are hard-partitioned by org: the partition key is "ORG#<orgID>".
// Every query is pinned to a single org's partition, so a read can never cross
// the tenant boundary. Secrets are stored as a plain attribute — encrypt at rest
// (KMS/SSE) and never project them into logs.
//
// # Schema
//
//	Subscriptions table: PK "ORG#<orgID>", SK "SUB#<subID>"
//	Deliveries    table: PK "ORG#<orgID>", SK "DLV#<deliveryID>"
//
// No secondary indexes are required; ListForOrgAndType filters the org partition
// in memory (fine for the modest per-org subscription counts webhooks imply).
package dynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// API is the subset of the DynamoDB client both adapters use. *dynamodb.Client
// satisfies it; it also makes the adapters mockable.
type API interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

func orgPK(orgID string) string { return "ORG#" + orgID }
func subSK(subID string) string { return "SUB#" + subID }
func dlvSK(id string) string    { return "DLV#" + id }

func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }

func key(pk, sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"PK": s(pk), "SK": s(sk)}
}

func marshal(v any) (map[string]types.AttributeValue, error) {
	m, err := attributevalue.MarshalMap(v)
	if err != nil {
		return nil, fmt.Errorf("dynamo: marshal: %w", err)
	}
	return m, nil
}
