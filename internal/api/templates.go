package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"event-driven-notification-system/internal/domain"
	tpl "event-driven-notification-system/internal/template"
)

func (a *API) templateRoutes(r chi.Router) {
	const base = "/api/v1/templates"
	r.Post(base, a.saveTemplate)
	r.Put(base+"/{id}", a.saveTemplate)
	r.Delete(base+"/{id}", func(w http.ResponseWriter, r *http.Request) {
		if id := resourceID(w, r); id != "" {
			if e := a.S.Store.DeleteTemplate(r.Context(), id); e != nil {
				failure(w, r, e)
				return
			}
			w.WriteHeader(204)
		}
	})
	r.Get(base, func(w http.ResponseWriter, r *http.Request) {
		v, e := a.S.Store.Templates(r.Context())
		if e != nil {
			failure(w, r, e)
			return
		}
		jsonResponse(w, 200, v)
	})
	r.Get(base+"/{id}", func(w http.ResponseWriter, r *http.Request) {
		if id := resourceID(w, r); id != "" {
			v, e := a.S.Store.Template(r.Context(), id)
			if e != nil {
				failure(w, r, e)
				return
			}
			jsonResponse(w, 200, v)
		}
	})
}
func (a *API) saveTemplate(w http.ResponseWriter, r *http.Request) {
	var t domain.NotificationTemplate
	if e := decode(w, r, 4<<20, &t); e != nil {
		problem(w, r, 400, e.Error())
		return
	}
	if e := tpl.Validate(t); e != nil {
		problem(w, r, 400, e.Error())
		return
	}
	create := r.Method == "POST"
	code := 201
	if !create {
		t.ID = resourceID(w, r)
		if t.ID == "" {
			return
		}
		code = 200
	}
	v, e := a.S.Store.SaveTemplate(r.Context(), t, create)
	if e != nil {
		failure(w, r, e)
		return
	}
	jsonResponse(w, code, v)
}

type invalidTemplate struct{ error }

func (a *API) render(ctx context.Context, item *CreateRequest, cache map[string]domain.NotificationTemplate) error {
	if item.TemplateID == "" {
		if len(item.Variables) > 0 {
			return invalidTemplate{errors.New("variables require templateId")}
		}
		return nil
	}
	if _, e := uuid.Parse(item.TemplateID); e != nil || item.Content != "" {
		return invalidTemplate{errors.New("templateId requires a valid UUID and omitted content")}
	}
	t, ok := cache[item.TemplateID]
	if !ok {
		var e error
		t, e = a.S.Store.Template(ctx, item.TemplateID)
		if e != nil {
			return e
		}
		cache[item.TemplateID] = t
	}
	if item.Channel != "" && item.Channel != t.Channel {
		return invalidTemplate{errors.New("template channel mismatch")}
	}
	item.Channel = t.Channel
	values := item.Variables
	if values == nil {
		values = map[string]string{}
	}
	content, e := tpl.Render(t.Body, values, 500<<10)
	if e != nil {
		return invalidTemplate{e}
	}
	item.Content = content
	if t.Subject != "" {
		v, e := tpl.Render(t.Subject, values, 1020)
		if e != nil {
			return invalidTemplate{e}
		}
		if item.Metadata == nil {
			item.Metadata = map[string]json.RawMessage{}
		}
		item.Metadata["subject"], _ = json.Marshal(v)
	}
	return nil
}
