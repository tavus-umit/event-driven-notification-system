package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"unicode/utf8"
)

// Decode the array incrementally so the 1,001st item can be rejected before
// the remainder of a large request is read or allocated.
func readBatch(w http.ResponseWriter, r *http.Request) (batchRequest, error) {
	var req batchRequest
	input := http.MaxBytesReader(w, r.Body, 1<<30)
	bounded := &io.LimitedReader{R: input, N: 4 << 20}
	d := json.NewDecoder(bounded)
	token, e := d.Token()
	if e != nil || token != json.Delim('{') {
		return req, errors.New("expected a batch object")
	}
	seen := map[string]bool{}
	for d.More() {
		bounded.N = 4 << 20
		token, e = d.Token()
		if e != nil {
			return req, e
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return req, errors.New("invalid batch field")
		}
		seen[key] = true
		switch key {
		case "batchId":
			if e = d.Decode(&req.ID); e != nil {
				return req, e
			}
		case "notifications":
			token, e = d.Token()
			if e != nil || token != json.Delim('[') {
				return req, errors.New("expected notifications array")
			}
			for d.More() {
				if len(req.Items) == 1000 {
					return req, errors.New("batch exceeds 1000 notifications")
				}
				bounded.N = 4 << 20
				var raw json.RawMessage
				if e = d.Decode(&raw); e != nil {
					return req, e
				}
				if len(raw) > 4<<20 || !utf8.Valid(raw) {
					return req, errors.New("invalid notification size or UTF-8")
				}
				var item CreateRequest
				one := json.NewDecoder(bytes.NewReader(raw))
				one.DisallowUnknownFields()
				if e = one.Decode(&item); e != nil {
					return req, e
				}
				req.Items = append(req.Items, item)
			}
			if _, e = d.Token(); e != nil {
				return req, e
			}
		default:
			return req, fmt.Errorf("unknown batch field %q", key)
		}
	}
	if _, e = d.Token(); e != nil {
		return req, e
	}
	if d.Decode(new(any)) != io.EOF {
		return req, errors.New("trailing batch data")
	}
	return req, nil
}
