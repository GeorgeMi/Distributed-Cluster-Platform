---
name: Professor feedback on documentation and implementation
description: Key corrections and suggestions from prof. Buțincu on diagrams, WeightedLB description, JWT refresh tokens
type: feedback
---

1. WeightedLB: don't say "converted to CPU-equivalent units" - say "weighted score between available CPU and available RAM". Also mention the pluggable API so anyone can write their own LoadBalancer implementation.
**Why:** The CPU-equivalent conversion doesn't make sense conceptually.
**How to apply:** Fix the LaTeX description in Chapter 3 and the code comment in weighted.go.

2. JWT: look into refresh tokens in addition to basic JWT.
**Why:** Production systems use refresh tokens for better security.
**How to apply:** Add refresh token support when implementing Security module.

3. Earlier feedback (applied): heartbeats should be UDP multicast not TCP/mTLS, no HeartbeatACK, no JoinRequest/LeaveRequest (auto-join via heartbeat), KillContainers instead of reject for split-brain, trusted network (no mTLS between worker/master), service parameters (envVars, ports, cmd) saved in Service entity.
