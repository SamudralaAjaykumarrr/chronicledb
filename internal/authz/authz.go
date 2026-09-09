// Package authz implements ChronicleDB's RBAC authorization layer
// (docs/enterprise-v1-plan.md §5 layer 5): "three fixed roles (admin,
// operator, read-only)... Role assignment is static per credential
// (token-to-role or cert-subject-to-role mapping), not a dynamic grants
// database."
package authz

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Role is one of ChronicleDB's three fixed V1 roles.
type Role string

const (
	// RoleAdmin may call any endpoint, including /fault and future
	// backup/membership/upgrade endpoints.
	RoleAdmin Role = "admin"
	// RoleOperator may call operational endpoints (/status, /propose,
	// /outcome, future backup-trigger) but not /fault or membership
	// changes.
	RoleOperator Role = "operator"
	// RoleReadOnly may call /status, /metrics, /health, and SQL
	// SELECT-only paths.
	RoleReadOnly Role = "read-only"
)

// ParseRole validates s as one of the three fixed roles.
func ParseRole(s string) (Role, error) {
	switch Role(s) {
	case RoleAdmin, RoleOperator, RoleReadOnly:
		return Role(s), nil
	default:
		return "", fmt.Errorf("authz: unknown role %q (must be one of %q, %q, %q)", s, RoleAdmin, RoleOperator, RoleReadOnly)
	}
}

// Endpoint identifiers used by the decision table below. Deliberately
// distinct from raw HTTP paths so the table has exactly one entry per
// logical administrative action, independent of exact route spelling.
const (
	EndpointStatus    = "status"
	EndpointMetrics   = "metrics"
	EndpointHealth    = "health"
	EndpointPropose   = "propose"
	EndpointOutcome   = "outcome"
	EndpointFault     = "fault"
	EndpointReloadTLS = "admin.reload-tls"
	// EndpointBackup gates /admin/backup (docs/enterprise-v1-plan.md §6,
	// §5's own RBAC section: "operator may call operational
	// endpoints... (future backup-trigger)").
	EndpointBackup = "admin.backup"
	// EndpointUpgradePrecheck gates /admin/upgrade/precheck
	// (docs/enterprise-v1-plan.md §7 "Security implications:
	// Precheck/finalize are admin-gated, audited actions").
	EndpointUpgradePrecheck = "admin.upgrade.precheck"
	// EndpointUpgradeFinalize gates /admin/upgrade/finalize
	// (docs/enterprise-v1-plan.md §7, same RBAC note as
	// EndpointUpgradePrecheck).
	EndpointUpgradeFinalize = "admin.upgrade.finalize"
)

// AllEndpoints lists every endpoint the decision table below covers —
// used by the RBAC decision-table test to assert every role × every
// endpoint pair is deliberately decided (docs/enterprise-v1-plan.md §5
// "Deterministic tests": "RBAC decision-table tests (every role x every
// endpoint)").
var AllEndpoints = []string{
	EndpointStatus, EndpointMetrics, EndpointHealth,
	EndpointPropose, EndpointOutcome,
	EndpointFault, EndpointReloadTLS,
	EndpointBackup,
	EndpointUpgradePrecheck, EndpointUpgradeFinalize,
}

// AllRoles lists every fixed V1 role.
var AllRoles = []Role{RoleAdmin, RoleOperator, RoleReadOnly}

// decisionTable is the single source of truth for RBAC: which roles may
// call which endpoint (docs/enterprise-v1-plan.md §5 layer 5, verbatim).
// admin implicitly may call everything operator/read-only can — encoded
// explicitly per endpoint below rather than via role hierarchy in code,
// so the table itself is the complete, auditable decision surface (no
// implicit inheritance logic to get wrong).
var decisionTable = map[string]map[Role]bool{
	EndpointStatus:  {RoleAdmin: true, RoleOperator: true, RoleReadOnly: true},
	EndpointMetrics: {RoleAdmin: true, RoleOperator: true, RoleReadOnly: true},
	EndpointHealth:  {RoleAdmin: true, RoleOperator: true, RoleReadOnly: true},
	EndpointPropose: {RoleAdmin: true, RoleOperator: true, RoleReadOnly: false},
	EndpointOutcome: {RoleAdmin: true, RoleOperator: true, RoleReadOnly: false},
	// /fault requires admin AND the -enable-fault-endpoint flag
	// (enforced structurally by conditional route registration in
	// cmd/chronicledb-node, not by this table alone — see
	// docs/enterprise-v1-plan.md §5 layer 8 and this package's doc
	// comment on Allowed).
	EndpointFault:     {RoleAdmin: true, RoleOperator: false, RoleReadOnly: false},
	EndpointReloadTLS: {RoleAdmin: true, RoleOperator: false, RoleReadOnly: false},
	// docs/enterprise-v1-plan.md §5's own RBAC layer description names
	// backup triggering explicitly as an operator-permitted operational
	// action, unlike /fault or membership changes.
	EndpointBackup: {RoleAdmin: true, RoleOperator: true, RoleReadOnly: false},
	// Precheck/finalize are admin-only, unlike backup-trigger
	// (docs/enterprise-v1-plan.md §7 "Security implications:
	// Precheck/finalize are admin-gated, audited actions") — an operator
	// may trigger a backup but may not change the cluster's
	// version-compatibility boundary.
	EndpointUpgradePrecheck: {RoleAdmin: true, RoleOperator: false, RoleReadOnly: false},
	EndpointUpgradeFinalize: {RoleAdmin: true, RoleOperator: false, RoleReadOnly: false},
}

// Allowed reports whether role may call endpoint, per the fixed V1 RBAC
// decision table. An unknown endpoint is always denied (fail closed):
// a typo or a forgotten table entry for a newly added endpoint must
// never silently default to "allowed."
//
// Allowed does not, by itself, implement FAULT SURFACE OFF BY DEFAULT —
// that additionally requires /fault's route to be structurally
// unregistered unless -enable-fault-endpoint is set, which only
// cmd/chronicledb-node's mux wiring can do; Allowed only ever answers
// "if this endpoint is reachable at all, may role call it."
func Allowed(endpoint string, role Role) bool {
	roles, ok := decisionTable[endpoint]
	if !ok {
		return false
	}
	return roles[role]
}

// LoadTokenFile parses an -auth-token-file: each non-empty,
// non-comment (#-prefixed) line is "<token>:<principal>". Returns
// token -> principal name, ready for authn.NewTokenAuthenticator.
func LoadTokenFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("authz: opening auth token file %s: %w", path, err)
	}
	defer f.Close()

	tokens := make(map[string]string)
	sc := bufio.NewScanner(f)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		parts := strings.SplitN(text, ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("authz: auth token file %s line %d: expected \"<token>:<principal>\", got %q", path, line, text)
		}
		tokens[parts[0]] = parts[1]
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("authz: reading auth token file %s: %w", path, err)
	}
	return tokens, nil
}

// LoadMappingFile parses an -rbac-mapping-file: a JSON object mapping
// principal name (a token file's principal, or an mTLS certificate's
// CommonName) to one of the three fixed role names. Used uniformly for
// both token and mTLS authentication modes.
func LoadMappingFile(path string) (map[string]Role, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("authz: reading RBAC mapping file %s: %w", path, err)
	}
	var raw map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("authz: parsing RBAC mapping file %s: %w", path, err)
	}
	mapping := make(map[string]Role, len(raw))
	for principal, roleStr := range raw {
		role, err := ParseRole(roleStr)
		if err != nil {
			return nil, fmt.Errorf("authz: RBAC mapping file %s: principal %q: %w", path, principal, err)
		}
		mapping[principal] = role
	}
	return mapping, nil
}
