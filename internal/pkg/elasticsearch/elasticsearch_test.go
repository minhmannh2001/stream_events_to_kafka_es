package elasticsearch

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
)

type TestDocument struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func setupTestServer(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	server := httptest.NewServer(handler)

	cfg := elasticsearch.Config{
		Addresses: []string{server.URL},
	}

	es, err := elasticsearch.NewClient(cfg)
	assert.NoError(t, err)

	client := &Client{es: es}
	return client, server
}

func TestBulkIndexDocuments(t *testing.T) {
	client, server := setupTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/_bulk", r.URL.Path)
		assert.Equal(t, "POST", r.Method)

		var bulkRequest string
		buffer := make([]byte, 1024)
		n, _ := r.Body.Read(buffer)
		bulkRequest = string(buffer[:n])

		assert.Contains(t, bulkRequest, `{"index":{"_index":"test-index"}}`)
		assert.Contains(t, bulkRequest, `{"id":"1","name":"Test1"}`)
		assert.Contains(t, bulkRequest, `{"id":"2","name":"Test2"}`)

		w.Header().Add("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"took": 30,
			"errors": false,
			"items": [
				{"index": {"_index": "test-index", "_id": "1", "status": 201}},
				{"index": {"_index": "test-index", "_id": "2", "status": 201}}
			]
		}`))
	})
	defer server.Close()

	docs := []interface{}{
		TestDocument{ID: "1", Name: "Test1"},
		TestDocument{ID: "2", Name: "Test2"},
	}

	err := client.BulkIndexDocuments("test-index", docs)
	assert.NoError(t, err)
}
