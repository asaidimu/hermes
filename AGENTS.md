## Writing TODOs

When starting on a new taks, write the steps to a file under the `todo` folder.

### Rules & Best Practices

* **Provide Enough Context:** Because tasks may be picked up by another agent or across different sessions, **never create single-phrase TODOs**. Each task must include enough background, intent, relevant file paths, links, or specific requirements so any agent can take over seamlessly without asking for context.
* **Track State Clearly:** Update task statuses promptly as work progresses.

### Task Status Identifiers

* `[ ]` Tasks that are planned.
* `[-]` Tasks that are in progress.
* `[*]` Tasks that are done.
* `[=]` Tasks that are skipped (include brief reasoning).
* `[X]` Tasks that are blocked (include the blocker details).

### Format Example

```markdown
- [ ] Implement user auth middleware
  - **Context:** Required for protecting `/api/v1/dashboard` routes.
  - **Details:** Use JWT validation based on existing spec in `docs/auth.md`.
  - **Files:** Modify `src/middleware/auth.js` and add unit tests to `tests/auth.test.js`.
```

