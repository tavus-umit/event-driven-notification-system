package template

import "testing"

func TestRender(t *testing.T) {
	for _, tc := range []struct {
		body, want string
		ok         bool
	}{{"Hi {{ name }}", "Hi {{other}}", true}, {"{{missing}}", "", false}, {"{{name", "", false}, {"a}}", "", false}, {"{{.Name}}", "", false}} {
		v, e := Render(tc.body, map[string]string{"name": "{{other}}"}, 100)
		if (e == nil) != tc.ok || v != tc.want {
			t.Fatal(tc, v, e)
		}
	}
	if _, e := Render("{{name}}{{name}}", map[string]string{"name": "abc"}, 5); e == nil {
		t.Fatal("expansion exceeds limit")
	}
}
