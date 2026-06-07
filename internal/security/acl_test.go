package security

import (
	"testing"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
)

func TestAdminCanDoEverything(t *testing.T) {
	cases := []struct{ resource, action string }{
		{"services", "create"},
		{"services", "delete"},
		{"services", "read"},
		{"nodes", "read"},
		{"users", "create"},
		{"audit", "read"},
	}
	for _, c := range cases {
		if !CheckPermission(domain.RoleAdmin, c.resource, c.action) {
			t.Errorf("admin should be able to %s %s", c.action, c.resource)
		}
	}
}

func TestReaderCannotCreate(t *testing.T) {
	if CheckPermission(domain.RoleReader, "services", "create") {
		t.Error("reader should not be able to create services")
	}
	if CheckPermission(domain.RoleReader, "services", "delete") {
		t.Error("reader should not be able to delete services")
	}
}

func TestWriterCannotManageUsers(t *testing.T) {
	if CheckPermission(domain.RoleWriter, "users", "create") {
		t.Error("writer should not be able to create users")
	}
}

func TestInvalidRole(t *testing.T) {
	if CheckPermission("UNKNOWN", "services", "read") {
		t.Error("unknown role should have no permissions")
	}
}
