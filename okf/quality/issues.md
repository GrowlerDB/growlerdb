---
type: Process
title: How issues are handled
description: Where issues are tracked and the conventions for triaging them.
tags: [quality, issues, process, triage]
timestamp: 2026-07-04T14:22:00
---

# How issues are handled

How GrowlerDB tracks and triages problems — **not** a list of open issues.

## Tracking

- **User-facing bugs and feature requests** live in **GitHub Issues** on the
  [repository](/product/interfaces/git-repo.md), labelled by area/severity and triaged into work.
- **Planned/roadmap work** lives in a **backlog** (task files) kept **outside** the published bundle —
  each task is a small, self-contained slice with acceptance criteria and a resolution note.
- **Durable caveats and known gaps** (things that are working-as-designed but limited) are recorded as
  [known limitations](/quality/known-limitations/index.md), so they aren't lost in an issue tracker.

## Conventions

An issue is triaged to: fix-now (a slice), backlog (later), or known-limitation (documented gap). A
change that fixes an issue ships with tests that would have caught it, and updates the relevant OKF
concept(s) per the [workflow](/workflow.md).
