package dynamo

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// Store is the DynamoDB events.SubscriptionStore adapter.
type Store struct {
	api   API
	table string
}

var _ events.SubscriptionStore = (*Store)(nil)

// NewStore returns a subscription store backed by the given table.
func NewStore(api API, table string) *Store { return &Store{api: api, table: table} }

// subItem is the persisted shape of a subscription (plus its keys).
type subItem struct {
	PK             string                 `dynamodbav:"PK"`
	SK             string                 `dynamodbav:"SK"`
	ID             string                 `dynamodbav:"id"`
	OrgID          string                 `dynamodbav:"orgId"`
	EventTypes     []string               `dynamodbav:"eventTypes"`
	URL            string                 `dynamodbav:"url"`
	Secret         string                 `dynamodbav:"secret"`
	Active         bool                   `dynamodbav:"active"`
	CreatedAt      time.Time              `dynamodbav:"createdAt"`
	PayloadFilters []events.PayloadFilter `dynamodbav:"payloadFilters,omitempty"`
}

func toSubItem(sub events.Subscription) subItem {
	return subItem{
		PK: orgPK(sub.OrgID), SK: subSK(sub.ID),
		ID: sub.ID, OrgID: sub.OrgID, EventTypes: sub.EventTypes,
		URL: sub.URL, Secret: sub.Secret, Active: sub.Active, CreatedAt: sub.CreatedAt,
		PayloadFilters: sub.PayloadFilters,
	}
}

func (it subItem) toSub() events.Subscription {
	return events.Subscription{
		ID: it.ID, OrgID: it.OrgID, EventTypes: it.EventTypes,
		URL: it.URL, Secret: it.Secret, Active: it.Active, CreatedAt: it.CreatedAt,
		PayloadFilters: it.PayloadFilters,
	}
}

func (st *Store) Put(ctx context.Context, sub events.Subscription) error {
	item, err := marshal(toSubItem(sub))
	if err != nil {
		return err
	}
	_, err = st.api.PutItem(ctx, &dynamodb.PutItemInput{TableName: &st.table, Item: item})
	return err
}

func (st *Store) Get(ctx context.Context, orgID, subID string) (events.Subscription, error) {
	out, err := st.api.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &st.table,
		Key:       key(orgPK(orgID), subSK(subID)),
	})
	if err != nil {
		return events.Subscription{}, err
	}
	if out.Item == nil {
		return events.Subscription{}, events.ErrNotFound
	}
	var it subItem
	if err := attributevalue.UnmarshalMap(out.Item, &it); err != nil {
		return events.Subscription{}, fmt.Errorf("dynamo: unmarshal: %w", err)
	}
	return it.toSub(), nil
}

func (st *Store) Delete(ctx context.Context, orgID, subID string) error {
	_, err := st.api.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &st.table,
		Key:       key(orgPK(orgID), subSK(subID)),
	})
	return err
}

func (st *Store) ListForOrg(ctx context.Context, orgID string) ([]events.Subscription, error) {
	return st.queryOrg(ctx, orgID, "")
}

func (st *Store) ListForOrgAndType(ctx context.Context, orgID, eventType string) ([]events.Subscription, error) {
	all, err := st.queryOrg(ctx, orgID, "")
	if err != nil {
		return nil, err
	}
	var out []events.Subscription
	for _, sub := range all {
		if sub.Matches(eventType) {
			out = append(out, sub)
		}
	}
	return out, nil
}

// queryOrg pages through an org's SUB# items.
func (st *Store) queryOrg(ctx context.Context, orgID, _ string) ([]events.Subscription, error) {
	var out []events.Subscription
	var start map[string]types.AttributeValue
	for {
		res, err := st.api.Query(ctx, &dynamodb.QueryInput{
			TableName:              &st.table,
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :sk)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": s(orgPK(orgID)),
				":sk": s("SUB#"),
			},
			ExclusiveStartKey: start,
		})
		if err != nil {
			return nil, err
		}
		for _, raw := range res.Items {
			var it subItem
			if err := attributevalue.UnmarshalMap(raw, &it); err != nil {
				return nil, fmt.Errorf("dynamo: unmarshal: %w", err)
			}
			out = append(out, it.toSub())
		}
		if len(res.LastEvaluatedKey) == 0 {
			break
		}
		start = res.LastEvaluatedKey
	}
	return out, nil
}
