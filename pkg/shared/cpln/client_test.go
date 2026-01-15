package cpln

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFindNextLink(t *testing.T) {
	tests := []struct {
		name  string
		links []Link
		want  string
	}{
		{
			name:  "has next link",
			links: []Link{{Rel: "self", Href: "/self"}, {Rel: "next", Href: "/next"}},
			want:  "/next",
		},
		{
			name:  "no next link",
			links: []Link{{Rel: "self", Href: "/self"}},
			want:  "",
		},
		{
			name:  "empty links",
			links: []Link{},
			want:  "",
		},
		{
			name:  "nil links",
			links: nil,
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findNextLink(tt.links)
			if got != tt.want {
				t.Errorf("findNextLink() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestQueryCommands_SinglePage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(CommandList{
			Kind: "list",
			Items: []Command{
				{ID: "cmd-1", Type: "runCronWorkload", LifecycleStage: "completed"},
				{ID: "cmd-2", Type: "runCronWorkload", LifecycleStage: "running"},
			},
			Links: []Link{{Rel: "self", Href: "/query"}}, // No next link
		})
	}))
	defer server.Close()

	client := &Client{token: "test", org: "test-org", baseURL: server.URL, client: http.DefaultClient}
	result, err := client.QueryCommands("gvc", "workload", nil, 0)

	if err != nil {
		t.Fatalf("QueryCommands() error: %v", err)
	}
	if len(result.Items) != 2 {
		t.Errorf("got %d items, want 2", len(result.Items))
	}
}

func TestQueryCommands_MultiplePagesNoLimit(t *testing.T) {
	pageCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pageCount++
		w.Header().Set("Content-Type", "application/json")

		if strings.Contains(r.URL.Path, "-query") && r.Method == "POST" {
			// First page from POST query
			json.NewEncoder(w).Encode(CommandList{
				Kind:  "list",
				Items: []Command{{ID: "cmd-1"}},
				Links: []Link{{Rel: "next", Href: "/page2"}},
			})
		} else if r.URL.Path == "/page2" {
			// Second page
			json.NewEncoder(w).Encode(CommandList{
				Kind:  "list",
				Items: []Command{{ID: "cmd-2"}},
				Links: []Link{{Rel: "next", Href: "/page3"}},
			})
		} else if r.URL.Path == "/page3" {
			// Third page - no more
			json.NewEncoder(w).Encode(CommandList{
				Kind:  "list",
				Items: []Command{{ID: "cmd-3"}},
				Links: []Link{{Rel: "self", Href: "/page3"}},
			})
		}
	}))
	defer server.Close()

	client := &Client{token: "test", org: "test-org", baseURL: server.URL, client: http.DefaultClient}
	result, err := client.QueryCommands("gvc", "workload", nil, 0)

	if err != nil {
		t.Fatalf("QueryCommands() error: %v", err)
	}
	if len(result.Items) != 3 {
		t.Errorf("got %d items, want 3", len(result.Items))
	}
	if pageCount != 3 {
		t.Errorf("fetched %d pages, want 3", pageCount)
	}
}

func TestQueryCommands_WithLimit(t *testing.T) {
	pageCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pageCount++
		w.Header().Set("Content-Type", "application/json")

		if strings.Contains(r.URL.Path, "-query") && r.Method == "POST" {
			json.NewEncoder(w).Encode(CommandList{
				Kind:  "list",
				Items: []Command{{ID: "cmd-1"}, {ID: "cmd-2"}},
				Links: []Link{{Rel: "next", Href: "/page2"}},
			})
		} else if r.URL.Path == "/page2" {
			json.NewEncoder(w).Encode(CommandList{
				Kind:  "list",
				Items: []Command{{ID: "cmd-3"}, {ID: "cmd-4"}},
				Links: []Link{{Rel: "next", Href: "/page3"}},
			})
		} else {
			// Should not reach here with limit=3
			t.Error("fetched more pages than expected")
		}
	}))
	defer server.Close()

	client := &Client{token: "test", org: "test-org", baseURL: server.URL, client: http.DefaultClient}
	result, err := client.QueryCommands("gvc", "workload", nil, 3)

	if err != nil {
		t.Fatalf("QueryCommands() error: %v", err)
	}
	if len(result.Items) != 3 {
		t.Errorf("got %d items, want 3 (limited)", len(result.Items))
	}
	// Should only fetch 2 pages (first page has 2 items, need 1 more)
	if pageCount != 2 {
		t.Errorf("fetched %d pages, want 2", pageCount)
	}
}

func TestQueryActiveCommands_FetchesAllPages(t *testing.T) {
	pageCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pageCount++
		w.Header().Set("Content-Type", "application/json")

		if r.Method == "POST" {
			json.NewEncoder(w).Encode(CommandList{
				Kind:  "list",
				Items: []Command{{ID: "active-1", LifecycleStage: "running"}},
				Links: []Link{{Rel: "next", Href: "/page2"}},
			})
		} else {
			json.NewEncoder(w).Encode(CommandList{
				Kind:  "list",
				Items: []Command{{ID: "active-2", LifecycleStage: "pending"}},
				Links: []Link{},
			})
		}
	}))
	defer server.Close()

	client := &Client{token: "test", org: "test-org", baseURL: server.URL, client: http.DefaultClient}
	result, err := client.QueryActiveCommands("gvc", "workload", 0)

	if err != nil {
		t.Fatalf("QueryActiveCommands() error: %v", err)
	}
	if len(result.Items) != 2 {
		t.Errorf("got %d items, want 2", len(result.Items))
	}
}

func TestQueryAllCommands_WithLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Return more items than limit
		json.NewEncoder(w).Encode(CommandList{
			Kind: "list",
			Items: []Command{
				{ID: "cmd-1"}, {ID: "cmd-2"}, {ID: "cmd-3"},
				{ID: "cmd-4"}, {ID: "cmd-5"}, {ID: "cmd-6"},
			},
			Links: []Link{{Rel: "next", Href: "/more"}}, // Has more pages
		})
	}))
	defer server.Close()

	client := &Client{token: "test", org: "test-org", baseURL: server.URL, client: http.DefaultClient}
	result, err := client.QueryAllCommands("gvc", "workload", 5)

	if err != nil {
		t.Fatalf("QueryAllCommands() error: %v", err)
	}
	if len(result.Items) != 5 {
		t.Errorf("got %d items, want 5 (limited)", len(result.Items))
	}
}

func TestFetchPages_StopsWhenLimitReached(t *testing.T) {
	// Test that fetchPages stops fetching when limit is reached mid-page
	pageCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pageCount++
		w.Header().Set("Content-Type", "application/json")

		// Each page returns 3 items
		if pageCount == 1 {
			json.NewEncoder(w).Encode(CommandList{
				Kind:  "list",
				Items: []Command{{ID: "cmd-1"}, {ID: "cmd-2"}, {ID: "cmd-3"}},
				Links: []Link{{Rel: "next", Href: "/page2"}},
			})
		} else {
			// Should not reach here with limit=2
			t.Error("fetched more pages than expected with limit=2")
		}
	}))
	defer server.Close()

	client := &Client{token: "test", org: "test-org", baseURL: server.URL, client: http.DefaultClient}
	result, err := client.QueryCommands("gvc", "workload", nil, 2)

	if err != nil {
		t.Fatalf("QueryCommands() error: %v", err)
	}
	// First page has 3 items, but we only want 2
	if len(result.Items) != 2 {
		t.Errorf("got %d items, want 2 (limited)", len(result.Items))
	}
}
