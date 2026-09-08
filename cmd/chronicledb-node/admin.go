package main

import (
	"errors"
	"log"
	"net/http"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/identity"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/node"
)

// reloadTLS implements docs/enterprise-v1-plan.md §5 layer 7:
// certificate rotation via an admin-only /admin/reload-tls endpoint (in
// addition to SIGHUP — see main.go). It reloads whichever TLS material
// is actually configured on this process (peer mTLS via n.ReloadPeerTLS,
// client-facing TLS via clientHolder.Reload) — reloading is a no-op,
// not an error, for whichever surface was never configured, since a
// deployment may legitimately have only one of the two enabled.
func reloadTLS(n *node.Node, clientHolder *identity.Holder, logger *log.Logger) error {
	var errs []error
	if err := n.ReloadPeerTLS(); err != nil && !errors.Is(err, node.ErrPeerTLSNotConfigured) {
		errs = append(errs, err)
	}
	if clientHolder != nil {
		if err := clientHolder.Reload(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if logger != nil {
		logger.Printf("TLS material reloaded")
	}
	return nil
}

func (s *controlServer) handleReloadTLS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if err := reloadTLS(s.n, s.clientTLSHolder, s.logger); err != nil {
		http.Error(w, "reload failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
