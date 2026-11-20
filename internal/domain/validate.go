package domain

import (
	"encoding/json"
	"errors"
	"net/mail"
	"regexp"
	"strings"
	"unicode/utf8"
)

var phone = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)
var token = regexp.MustCompile(`^[A-Za-z0-9_+/=:-]{32,256}$`)

func (n Notification) Validate() error {
	if !n.Priority.Valid() || !n.Channel.Valid() {
		return errors.New("invalid channel or priority")
	}
	if strings.TrimSpace(n.Content) == "" || !utf8.ValidString(n.Content) || strings.ContainsRune(n.Content, 0) {
		return errors.New("content must be nonempty UTF-8 text")
	}
	if len(n.Recipient) > 512 {
		return errors.New("recipient exceeds 512 bytes")
	}
	switch n.Channel {
	case SMS:
		if !phone.MatchString(n.Recipient) || utf8.RuneCountInString(n.Content) > 1600 {
			return errors.New("SMS requires an E.164 recipient and at most 1600 characters")
		}
	case Email:
		a, err := mail.ParseAddress(n.Recipient)
		if err != nil || a.Address != n.Recipient || len(n.Content) > 500*1024 {
			return errors.New("invalid email address or body exceeds 500 KiB")
		}
	case Push:
		if !token.MatchString(n.Recipient) || utf8.RuneCountInString(n.Content) > 2048 {
			return errors.New("invalid push token or body exceeds 2048 characters")
		}
	}
	for key, limit := range map[string]int{"subject": 255, "title": 128} {
		if v, ok := n.Metadata[key]; ok {
			var s string
			if json.Unmarshal(v, &s) != nil || strings.TrimSpace(s) == "" || utf8.RuneCountInString(s) > limit {
				return errors.New("invalid " + key)
			}
		}
	}
	if data, ok := n.Metadata["data"]; ok && n.Channel == Push {
		var obj map[string]any
		if len(data) > 4096 || json.Unmarshal(data, &obj) != nil || obj == nil {
			return errors.New("push data must be an object up to 4096 bytes")
		}
	}
	return nil
}
