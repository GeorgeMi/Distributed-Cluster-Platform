package security

import "github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"

// Permission defines an action on a resource.
type Permission struct {
	Resource string // "services", "nodes", "pools", "users", "audit"
	Action   string // "read", "create", "delete"
}

// ACL rules: which roles can perform which actions.
var aclRules = map[string][]Permission{
	domain.RoleAdmin: {
		{Resource: "services", Action: "read"},
		{Resource: "services", Action: "create"},
		{Resource: "services", Action: "delete"},
		{Resource: "nodes", Action: "read"},
		{Resource: "pools", Action: "read"},
		{Resource: "users", Action: "read"},
		{Resource: "users", Action: "create"},
		{Resource: "audit", Action: "read"},
	},
	domain.RoleWriter: {
		{Resource: "services", Action: "read"},
		{Resource: "services", Action: "create"},
		{Resource: "services", Action: "delete"},
		{Resource: "nodes", Action: "read"},
		{Resource: "pools", Action: "read"},
		{Resource: "audit", Action: "read"},
	},
	domain.RoleReader: {
		{Resource: "services", Action: "read"},
		{Resource: "nodes", Action: "read"},
		{Resource: "pools", Action: "read"},
		{Resource: "audit", Action: "read"},
	},
}

// CheckPermission returns true if the role has permission for the action on the resource.
func CheckPermission(role, resource, action string) bool {
	perms, ok := aclRules[role]
	if !ok {
		return false
	}
	for _, p := range perms {
		if p.Resource == resource && p.Action == action {
			return true
		}
	}
	return false
}
