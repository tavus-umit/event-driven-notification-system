package template

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"event-driven-notification-system/internal/domain"
)

var variable = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Render accepts literal text and named substitutions only. Values are never parsed again.
func Render(body string, values map[string]string, maxBytes int) (string, error) {
	var out strings.Builder
	for len(body) > 0 {
		before, rest, found := strings.Cut(body, "{{")
		if strings.Contains(before, "}}") {
			return "", errors.New("unmatched template delimiter")
		}
		if out.Len()+len(before) > maxBytes {
			return "", errors.New("rendered text exceeds limit")
		}
		out.WriteString(before)
		if !found {
			break
		}
		name, tail, closed := strings.Cut(rest, "}}")
		name = strings.TrimSpace(name)
		if !closed || !variable.MatchString(name) {
			return "", errors.New("invalid template placeholder")
		}
		if values != nil {
			v, ok := values[name]
			if !ok {
				return "", fmt.Errorf("missing variable %s", name)
			}
			if out.Len()+len(v) > maxBytes {
				return "", errors.New("rendered text exceeds limit")
			}
			out.WriteString(v)
		}
		body = tail
	}
	return out.String(), nil
}
func Validate(t domain.NotificationTemplate) error {
	if strings.TrimSpace(t.Name) == "" || utf8.RuneCountInString(t.Name) > 128 || strings.ContainsRune(t.Name, 0) || !t.Channel.Valid() || strings.TrimSpace(t.Body) == "" || strings.ContainsRune(t.Body, 0) || strings.ContainsRune(t.Subject, 0) || utf8.RuneCountInString(t.Subject) > 255 {
		return errors.New("invalid template fields")
	}
	if _, e := Render(t.Subject, nil, 1024); e != nil {
		return e
	}
	_, e := Render(t.Body, nil, 500<<10)
	return e
}
