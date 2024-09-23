package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

type Client struct {
	es *elasticsearch.Client
}

func NewClient(addresses []string) (*Client, error) {
	cfg := elasticsearch.Config{
		Addresses: addresses,
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return &Client{es: es}, nil
}

// BulkIndexDocuments indexes multiple documents in Elasticsearch
func (c *Client) BulkIndexDocuments(index string, docs []interface{}) error {
	var buf bytes.Buffer

	for _, doc := range docs {
		// Action
		action := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": index,
			},
		}
		if err := json.NewEncoder(&buf).Encode(action); err != nil {
			return err
		}

		// Document
		if err := json.NewEncoder(&buf).Encode(doc); err != nil {
			return err
		}
	}

	res, err := c.es.Bulk(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error bulk indexing documents: %s", res.String())
	}

	var bulkResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return err
	}

	if bulkResponse["errors"].(bool) {
		return fmt.Errorf("bulk indexing encountered errors: %v", bulkResponse)
	}

	return nil
}

func (c *Client) BulkIndexDocumentsWithRetry(index string, docs []interface{}, retries int, retryInterval time.Duration) error {
	for attempt := 0; attempt < retries; attempt++ {
		var buf bytes.Buffer

		for _, doc := range docs {
			// Action
			action := map[string]interface{}{
				"index": map[string]interface{}{
					"_index": index,
				},
			}
			if err := json.NewEncoder(&buf).Encode(action); err != nil {
				return err
			}
			// Document
			if err := json.NewEncoder(&buf).Encode(doc); err != nil {
				return err
			}
		}
		res, err := c.es.Bulk(bytes.NewReader(buf.Bytes()))
		if err == nil {
			defer res.Body.Close()
			if !res.IsError() {
				var bulkResponse map[string]interface{}
				if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
					return err
				}
				if !bulkResponse["errors"].(bool) {
					return nil // Success!
				}
			}
		}

		// Retry logic
		if attempt < retries-1 {
			fmt.Printf("Bulk indexing failed (attempt %d/%d). Retrying in %v...\n", attempt+1, retries, retryInterval)
			time.Sleep(retryInterval)
		} else {
			return fmt.Errorf("bulk indexing failed after %d retries: %v", retries, err)
		}
	}

	// Should never reach here
	return nil
}
