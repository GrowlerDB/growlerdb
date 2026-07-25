## Summary

<!-- What does this change do, and why? Reference the backlog task if relevant. -->
<!-- PRs to `main` are whole features (feature-branch aggregation, D50). Decompose large work into
     sub-PRs into the feature branch, not into main. -->

## Checklist

- [ ] This PR to `main` is a **complete feature**, not a partial slice (feature-branch aggregation —
      [D50](../okf/system/decisions/d50-branching-model.md); slices go into the feature branch)
- [ ] `just check` passes (fmt, clippy `-D warnings`, tests, lint gates)
- [ ] Tests added/updated for the change
- [ ] **OKF updated** — the relevant `okf/` concept(s) reflect this change (behavior, interfaces,
      components, dependencies, decisions, or process). The OKF is the living source of truth; see
      [`okf/workflow.md`](../okf/workflow.md). *(N/A only for changes with no such impact.)*
- [ ] Docs updated where user-facing
