package main

import (
	"testing"
)

func TestBuildClientsURLFormat(t *testing.T) {
	config := Config{
		AgentPort:    "8080",
		WorkloadName: "demo-manticore",
		AuthToken:    "test-token",
	}
	server := &Server{config: config}

	clients := server.buildClients(3)

	expectedURLs := []string{
		"http://demo-manticore-0.demo-manticore:8080",
		"http://demo-manticore-1.demo-manticore:8080",
		"http://demo-manticore-2.demo-manticore:8080",
	}

	if len(clients) != len(expectedURLs) {
		t.Fatalf("expected %d clients, got %d", len(expectedURLs), len(clients))
	}

	for i, c := range clients {
		if c.BaseURL() != expectedURLs[i] {
			t.Errorf("client %d: expected URL %q, got %q", i, expectedURLs[i], c.BaseURL())
		}
	}
}

func TestBuildClientsStaticURLFormat(t *testing.T) {
	config := Config{
		AgentPort:    "8080",
		WorkloadName: "test-manticore",
		AuthToken:    "test-token",
	}

	clients := buildClientsStatic(config, 2)

	expectedURLs := []string{
		"http://test-manticore-0.test-manticore:8080",
		"http://test-manticore-1.test-manticore:8080",
	}

	if len(clients) != len(expectedURLs) {
		t.Fatalf("expected %d clients, got %d", len(expectedURLs), len(clients))
	}

	for i, c := range clients {
		if c.BaseURL() != expectedURLs[i] {
			t.Errorf("client %d: expected URL %q, got %q", i, expectedURLs[i], c.BaseURL())
		}
	}
}

func TestBuildClientsZeroReplicas(t *testing.T) {
	config := Config{
		AgentPort:    "8080",
		WorkloadName: "demo-manticore",
		AuthToken:    "test-token",
	}
	server := &Server{config: config}

	clients := server.buildClients(0)

	if len(clients) != 0 {
		t.Errorf("expected 0 clients, got %d", len(clients))
	}
}
