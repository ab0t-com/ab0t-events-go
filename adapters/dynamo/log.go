package dynamo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// Log is the DynamoDB events.DeliveryLog adapter. A delivery is one item; each
// attempt is appended to a list attribute via an atomic UpdateItem (list_append),
// and the event snapshot is written once (if_not_exists).
type Log struct {
	api   API
	table string
}

var _ events.DeliveryLog = (*Log)(nil)

// NewLog returns a delivery log backed by the given table.
func NewLog(api API, table string) *Log { return &Log{api: api, table: table} }

type attemptItem struct {
	N       int       `dynamodbav:"n"`
	At      time.Time `dynamodbav:"at"`
	Code    int       `dynamodbav:"code"`
	Latency int64     `dynamodbav:"latencyNs"`
	Error   string    `dynamodbav:"error,omitempty"`
}

type eventItem struct {
	ID         string            `dynamodbav:"id"`
	Type       string            `dynamodbav:"type"`
	OrgID      string            `dynamodbav:"orgId"`
	OccurredAt time.Time         `dynamodbav:"occurredAt"`
	Payload    []byte            `dynamodbav:"payload"`           // stored as Binary
	Headers    map[string]string `dynamodbav:"headers,omitempty"` // custom outbound headers
}

func (l *Log) Record(ctx context.Context, rec events.DeliveryRecord) error {
	d := rec.Delivery
	names := map[string]string{
		"#status": "status", "#att": "attemptCount", "#err": "lastError",
		"#upd": "updatedAt", "#et": "eventType", "#sub": "subscriptionId",
		"#eid": "eventId", "#org": "orgId", "#created": "createdAt", "#ev": "event",
	}
	values := map[string]types.AttributeValue{
		":status":  s(string(d.Status)),
		":att":     nInt(d.Attempts),
		":err":     s(d.LastError),
		":upd":     tsAttr(d.UpdatedAt),
		":et":      s(d.EventType),
		":sub":     s(d.SubscriptionID),
		":eid":     s(d.EventID),
		":org":     s(d.OrgID),
		":created": tsAttr(d.CreatedAt),
	}

	set := []string{
		"#status = :status", "#att = :att", "#err = :err", "#upd = :upd",
		"#et = :et", "#sub = :sub", "#eid = :eid", "#org = :org",
		"#created = if_not_exists(#created, :created)",
	}

	evAV, err := marshal(toEventItem(rec.Event))
	if err != nil {
		return err
	}
	values[":ev"] = &types.AttributeValueMemberM{Value: evAV}
	set = append(set, "#ev = if_not_exists(#ev, :ev)")

	if rec.Attempt.N > 0 {
		names["#attempts"] = "attempts"
		attAV, err := marshal(toAttemptItem(rec.Attempt))
		if err != nil {
			return err
		}
		values[":newAttempt"] = &types.AttributeValueMemberL{
			Value: []types.AttributeValue{&types.AttributeValueMemberM{Value: attAV}},
		}
		values[":empty"] = &types.AttributeValueMemberL{Value: []types.AttributeValue{}}
		set = append(set, "#attempts = list_append(if_not_exists(#attempts, :empty), :newAttempt)")
	}

	_, err = l.api.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &l.table,
		Key:                       key(orgPK(d.OrgID), dlvSK(d.ID)),
		UpdateExpression:          aws.String("SET " + strings.Join(set, ", ")),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	return err
}

func (l *Log) Get(ctx context.Context, orgID, deliveryID string) (events.Delivery, events.EventSnapshot, []events.Attempt, error) {
	out, err := l.api.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &l.table,
		Key:       key(orgPK(orgID), dlvSK(deliveryID)),
	})
	if err != nil {
		return events.Delivery{}, events.EventSnapshot{}, nil, err
	}
	if out.Item == nil {
		return events.Delivery{}, events.EventSnapshot{}, nil, events.ErrNotFound
	}
	return decodeDelivery(out.Item)
}

func (l *Log) List(ctx context.Context, orgID string, f events.DeliveryFilter) ([]events.Delivery, string, error) {
	limit := int32(f.Limit)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	in := &dynamodb.QueryInput{
		TableName:              &l.table,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :sk)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": s(orgPK(orgID)),
			":sk": s("DLV#"),
		},
		Limit: &limit,
	}
	if f.Cursor != "" {
		in.ExclusiveStartKey = key(orgPK(orgID), dlvSK(f.Cursor))
	}
	res, err := l.api.Query(ctx, in)
	if err != nil {
		return nil, "", err
	}
	var out []events.Delivery
	for _, raw := range res.Items {
		del, _, _, err := decodeDelivery(raw)
		if err != nil {
			return nil, "", err
		}
		if f.SubscriptionID != "" && del.SubscriptionID != f.SubscriptionID {
			continue
		}
		if f.Status != "" && del.Status != f.Status {
			continue
		}
		out = append(out, del)
	}
	next := ""
	if len(res.LastEvaluatedKey) != 0 {
		if sk, ok := res.LastEvaluatedKey["SK"].(*types.AttributeValueMemberS); ok {
			next = strings.TrimPrefix(sk.Value, "DLV#")
		}
	}
	return out, next, nil
}

// --- encode/decode helpers ---

func toAttemptItem(a events.Attempt) attemptItem {
	return attemptItem{N: a.N, At: a.At, Code: a.Code, Latency: int64(a.Latency), Error: a.Error}
}

func toEventItem(e events.EventSnapshot) eventItem {
	return eventItem{ID: e.ID, Type: e.Type, OrgID: e.OrgID, OccurredAt: e.OccurredAt, Payload: []byte(e.Payload), Headers: e.Headers}
}

func decodeDelivery(item map[string]types.AttributeValue) (events.Delivery, events.EventSnapshot, []events.Attempt, error) {
	var raw struct {
		ID        string        `dynamodbav:"-"`
		Status    string        `dynamodbav:"status"`
		Attempts  int           `dynamodbav:"attemptCount"`
		LastError string        `dynamodbav:"lastError"`
		UpdatedAt time.Time     `dynamodbav:"updatedAt"`
		CreatedAt time.Time     `dynamodbav:"createdAt"`
		EventType string        `dynamodbav:"eventType"`
		SubID     string        `dynamodbav:"subscriptionId"`
		EventID   string        `dynamodbav:"eventId"`
		OrgID     string        `dynamodbav:"orgId"`
		Event     eventItem     `dynamodbav:"event"`
		Attempt   []attemptItem `dynamodbav:"attempts"`
	}
	if err := attributevalue.UnmarshalMap(item, &raw); err != nil {
		return events.Delivery{}, events.EventSnapshot{}, nil, fmt.Errorf("dynamo: unmarshal: %w", err)
	}
	id := ""
	if sk, ok := item["SK"].(*types.AttributeValueMemberS); ok {
		id = strings.TrimPrefix(sk.Value, "DLV#")
	}
	del := events.Delivery{
		ID: id, OrgID: raw.OrgID, SubscriptionID: raw.SubID, EventID: raw.EventID,
		EventType: raw.EventType, Status: events.DeliveryStatus(raw.Status),
		Attempts: raw.Attempts, LastError: raw.LastError,
		CreatedAt: raw.CreatedAt, UpdatedAt: raw.UpdatedAt,
	}
	snap := events.EventSnapshot{
		ID: raw.Event.ID, Type: raw.Event.Type, OrgID: raw.Event.OrgID,
		OccurredAt: raw.Event.OccurredAt, Payload: raw.Event.Payload, Headers: raw.Event.Headers,
	}
	atts := make([]events.Attempt, 0, len(raw.Attempt))
	for _, a := range raw.Attempt {
		atts = append(atts, events.Attempt{N: a.N, At: a.At, Code: a.Code, Latency: time.Duration(a.Latency), Error: a.Error})
	}
	return del, snap, atts, nil
}

func nInt(v int) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", v)}
}

func tsAttr(t time.Time) types.AttributeValue {
	b, _ := t.MarshalText()
	return s(string(b))
}
