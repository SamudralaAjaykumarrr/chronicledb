package authz

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDecisionTable_EveryRoleEveryEndpoint is the RBAC decision-table
// test docs/enterprise-v1-plan.md §5 requires: "every role x every
// endpoint" is asserted explicitly, so a new endpoint added later
// without a table entry fails this test (via the fail-closed default in
// Allowed) rather than silently inheriting an unintended decision.
func TestDecisionTable_EveryRoleEveryEndpoint(t *testing.T) {
	want := map[string]map[Role]bool{
		EndpointStatus:          {RoleAdmin: true, RoleOperator: true, RoleReadOnly: true},
		EndpointMetrics:         {RoleAdmin: true, RoleOperator: true, RoleReadOnly: true},
		EndpointHealth:          {RoleAdmin: true, RoleOperator: true, RoleReadOnly: true},
		EndpointPropose:         {RoleAdmin: true, RoleOperator: true, RoleReadOnly: false},
		EndpointOutcome:         {RoleAdmin: true, RoleOperator: true, RoleReadOnly: false},
		EndpointFault:           {RoleAdmin: true, RoleOperator: false, RoleReadOnly: false},
		EndpointReloadTLS:       {RoleAdmin: true, RoleOperator: false, RoleReadOnly: false},
		EndpointBackup:          {RoleAdmin: true, RoleOperator: true, RoleReadOnly: false},
		EndpointUpgradePrecheck: {RoleAdmin: true, RoleOperator: false, RoleReadOnly: false},
		EndpointUpgradeFinalize: {RoleAdmin: true, RoleOperator: false, RoleReadOnly: false},
	}
	if len(want) != len(AllEndpoints) {
		t.Fatalf("this test's want table has %d endpoints, AllEndpoints has %d — keep them in sync", len(want), len(AllEndpoints))
	}
	for _, ep := range AllEndpoints {
		for _, role := range AllRoles {
			got := Allowed(ep, role)
			wantVal, ok := want[ep][role]
			if !ok {
				t.Fatalf("test table missing entry for endpoint=%s role=%s", ep, role)
			}
			if got != wantVal {
				t.Errorf("Allowed(%s, %s) = %v, want %v", ep, role, got, wantVal)
			}
		}
	}
}

func TestAllowed_UnknownEndpointDeniedForEveryRole(t *testing.T) {
	for _, role := range AllRoles {
		if Allowed("nonexistent-endpoint", role) {
			t.Errorf("Allowed(unknown endpoint, %s) = true, want false (fail closed)", role)
		}
	}
}

func TestAllowed_UnknownRoleDeniedForEveryEndpoint(t *testing.T) {
	for _, ep := range AllEndpoints {
		if Allowed(ep, Role("bogus")) {
			t.Errorf("Allowed(%s, bogus-role) = true, want false (fail closed)", ep)
		}
	}
}

func TestParseRole(t *testing.T) {
	for _, r := range AllRoles {
		got, err := ParseRole(string(r))
		if err != nil || got != r {
			t.Errorf("ParseRole(%q) = %v, %v; want %v, nil", r, got, err, r)
		}
	}
	if _, err := ParseRole("superuser"); err == nil {
		t.Fatal("ParseRole(superuser) should have failed")
	}
}

func TestLoadTokenFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tokens")
	content := "# comment\n\nabc123:alice\ndef456:bob\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	tokens, err := LoadTokenFile(path)
	if err != nil {
		t.Fatalf("LoadTokenFile: %v", err)
	}
	if tokens["abc123"] != "alice" || tokens["def456"] != "bob" || len(tokens) != 2 {
		t.Fatalf("tokens = %+v", tokens)
	}
}

func TestLoadTokenFile_MalformedLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tokens")
	if err := os.WriteFile(path, []byte("no-colon-here\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := LoadTokenFile(path); err == nil {
		t.Fatal("expected error for malformed token file line")
	}
}

func TestLoadMappingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rbac.json")
	content := `{"alice": "admin", "bob": "operator", "n1": "read-only"}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	m, err := LoadMappingFile(path)
	if err != nil {
		t.Fatalf("LoadMappingFile: %v", err)
	}
	if m["alice"] != RoleAdmin || m["bob"] != RoleOperator || m["n1"] != RoleReadOnly {
		t.Fatalf("mapping = %+v", m)
	}
}

func TestLoadMappingFile_InvalidRole(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rbac.json")
	content := `{"alice": "superuser"}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := LoadMappingFile(path); err == nil {
		t.Fatal("expected error for invalid role in mapping file")
	}
}
