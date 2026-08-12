package adminhttp

import (
	"crypto/rand"
	"encoding/hex"
	"html/template"
	"net/http"

	"github.com/ab0t-com/ab0t-events-go/events"
)

func generateSecret() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "whsec_" + hex.EncodeToString(b[:]), nil
}

// ui renders a minimal, self-contained HTML delivery-log view for the caller's
// org: recent deliveries with status, code, attempts, last error, and a Replay
// button per row. No external assets — safe to serve behind the same auth.
//
// CSRF: the Replay button submits a state-changing POST. If you serve this to
// browsers with cookie-based auth, put the handler behind your CSRF protection
// (token middleware) — the same as any other browser-facing mutation. With
// header/bearer auth it is not exploitable.
func (h *Handler) ui(w http.ResponseWriter, r *http.Request) {
	org, ok := h.auth(w, r)
	if !ok {
		return
	}
	deliveries, _, err := h.api.ListDeliveries(r.Context(), org, events.DeliveryFilter{
		SubscriptionID: r.URL.Query().Get("subscriptionId"),
		Status:         events.DeliveryStatus(r.URL.Query().Get("status")),
		Limit:          100,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uiTemplate.Execute(w, map[string]any{"Org": org, "Deliveries": deliveries}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "template error"})
	}
}

var uiTemplate = template.Must(template.New("ui").Funcs(template.FuncMap{
	"badge": func(s events.DeliveryStatus) template.CSS {
		switch s {
		case events.StatusDelivered:
			return "background:#e6f4ea;color:#137333"
		case events.StatusDead:
			return "background:#fce8e6;color:#c5221f"
		case events.StatusFailed:
			return "background:#fef7e0;color:#b06000"
		default:
			return "background:#e8eaed;color:#3c4043"
		}
	},
}).Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Webhook deliveries</title>
<style>
  :root { color-scheme: light dark; }
  body { font: 14px/1.5 system-ui, sans-serif; margin: 2rem; color: #202124; }
  h1 { font-size: 1.25rem; }
  .muted { color: #5f6368; }
  table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
  th, td { text-align: left; padding: .5rem .75rem; border-bottom: 1px solid #e0e0e0; vertical-align: top; }
  th { font-weight: 600; color: #5f6368; font-size: .8rem; text-transform: uppercase; letter-spacing: .03em; }
  .pill { display: inline-block; padding: .1rem .5rem; border-radius: 999px; font-size: .75rem; font-weight: 600; }
  code { font-family: ui-monospace, monospace; font-size: .85em; }
  .err { color: #c5221f; max-width: 32rem; }
  button { font: inherit; padding: .25rem .6rem; border: 1px solid #dadce0; border-radius: 6px; background: #fff; cursor: pointer; }
  button:hover { background: #f1f3f4; }
</style></head>
<body>
  <h1>Webhook deliveries <span class="muted">· org {{.Org}}</span></h1>
  {{if not .Deliveries}}<p class="muted">No deliveries yet.</p>{{end}}
  {{if .Deliveries}}
  <table>
    <thead><tr>
      <th>Delivery</th><th>Event</th><th>Status</th><th>Attempts</th><th>Last error</th><th></th>
    </tr></thead>
    <tbody>
    {{range .Deliveries}}
      <tr>
        <td><code>{{.ID}}</code><br><span class="muted">{{.SubscriptionID}}</span></td>
        <td>{{.EventType}}<br><span class="muted"><code>{{.EventID}}</code></span></td>
        <td><span class="pill" style="{{badge .Status}}">{{.Status}}</span></td>
        <td>{{.Attempts}}</td>
        <td class="err">{{.LastError}}</td>
        <td>
          <form method="post" action="deliveries/{{.ID}}/replay">
            <button type="submit">Replay</button>
          </form>
        </td>
      </tr>
    {{end}}
    </tbody>
  </table>
  {{end}}
</body></html>`))
