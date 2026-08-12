package dynamo

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// fakeDDB is a minimal in-memory stand-in for the DynamoDB client, sufficient to
// exercise the Store adapter's marshalling, key construction, and org-scoped
// Query filtering entirely offline (no AWS, no network).
type fakeDDB struct {
	items map[string]map[string]types.AttributeValue // "pk\x00sk" -> item
}

func newFakeDDB() *fakeDDB { return &fakeDDB{items: map[string]map[string]types.AttributeValue{}} }

func sv(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

func (f *fakeDDB) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.items[sv(in.Item["PK"])+"\x00"+sv(in.Item["SK"])] = in.Item
	return &dynamodb.PutItemOutput{}, nil
}
func (f *fakeDDB) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{Item: f.items[sv(in.Key["PK"])+"\x00"+sv(in.Key["SK"])]}, nil
}
func (f *fakeDDB) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	delete(f.items, sv(in.Key["PK"])+"\x00"+sv(in.Key["SK"]))
	return &dynamodb.DeleteItemOutput{}, nil
}
func (f *fakeDDB) UpdateItem(_ context.Context, _ *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}
func (f *fakeDDB) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	pk := sv(in.ExpressionAttributeValues[":pk"])
	prefix := sv(in.ExpressionAttributeValues[":sk"])
	var out []map[string]types.AttributeValue
	for _, item := range f.items {
		if sv(item["PK"]) == pk && strings.HasPrefix(sv(item["SK"]), prefix) {
			out = append(out, item)
		}
	}
	return &dynamodb.QueryOutput{Items: out}, nil
}

func TestStore_RoundTripAndOrgIsolation(t *testing.T) {
	ctx := context.Background()
	st := NewStore(newFakeDDB(), "subs")

	a := events.Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"drive.file.created"}, URL: "https://a/hook", Secret: "ka", Active: true}
	b := events.Subscription{ID: "s2", OrgID: "orgB", EventTypes: []string{"*"}, URL: "https://b/hook", Secret: "kb", Active: true}
	for _, sub := range []events.Subscription{a, b} {
		if err := st.Put(ctx, sub); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	got, err := st.Get(ctx, "orgA", "s1")
	if err != nil || got.URL != a.URL || got.Secret != a.Secret {
		t.Fatalf("get round-trip failed: %+v (%v)", got, err)
	}

	// org A's partition must not surface org B's subscription.
	list, err := st.ListForOrg(ctx, "orgA")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].ID != "s1" {
		t.Fatalf("org isolation broken: %+v", list)
	}

	// Type matching against the org partition.
	m, _ := st.ListForOrgAndType(ctx, "orgA", "drive.file.created")
	if len(m) != 1 {
		t.Fatalf("type match: got %d, want 1", len(m))
	}
	none, _ := st.ListForOrgAndType(ctx, "orgA", "mail.message.sent")
	if len(none) != 0 {
		t.Fatalf("unexpected match: %+v", none)
	}

	if err := st.Delete(ctx, "orgA", "s1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := st.Get(ctx, "orgA", "s1"); err != events.ErrNotFound {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
}

func TestStore_PayloadFiltersRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := NewStore(newFakeDDB(), "subs")
	sub := events.Subscription{
		ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a/hook", Secret: "k", Active: true,
		PayloadFilters: []events.PayloadFilter{{Path: "region", Equals: "eu"}, {Path: "tier", Equals: "gold"}},
	}
	if err := st.Put(ctx, sub); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := st.Get(ctx, "orgA", "s1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.PayloadFilters) != 2 || got.PayloadFilters[0] != (events.PayloadFilter{Path: "region", Equals: "eu"}) ||
		got.PayloadFilters[1] != (events.PayloadFilter{Path: "tier", Equals: "gold"}) {
		t.Fatalf("payload filters did not round-trip: %+v", got.PayloadFilters)
	}
}
