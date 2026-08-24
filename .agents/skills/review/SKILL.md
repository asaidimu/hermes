---
name: go-code-review
description: How to properly review codebases
---

# Code Review Guide for Go with SOLID and Devnotes

This guide combines **Go-specific best practices**, **SOLID design principles**, and **structured review notes using devnotes**. It is intended for reviewers and authors who want to produce maintainable, idiomatic, and well‑documented Go codebases.

---

## 1. Review Process Overview

1. **Automated checks first** – run formatting, vetting, linting, and tests (including race detector).
2. **Human review** – examine the diff for Go idioms, SOLID compliance, and overall design.
3. **Document findings** – use devnotes comments placed directly at the relevant code lines.
4. **Validate notes** – run `devnotes check` and `devnotes index update`.
5. **Author resolution** – the author edits notes manually (change status, add body) to reflect fixes.
6. **Verification** – reviewer queries open review notes to confirm all are resolved.

---

## 2. Automated Pre‑Review Checks

Run these before human review to eliminate mechanical issues:

```bash
gofmt -s -w .                # formatting
goimports -w .               # formatting + import management
go vet ./...                 # suspicious constructs
golangci-lint run            # additional linters (if configured)
go test ./...                # unit tests
go test -race ./...          # race detector
```

These catch many style and correctness issues, allowing the reviewer to focus on design.

---

## 3. Go‑Specific Review Checklist

### Naming & Style

- Use initialisms: `ID`, `URL`, `HTTP` (not `Id`, `Url`, `Http`).
- Avoid stutter: `user.UserName` → `user.Name`.
- Package names: short, lowercase, no underscores.
- Exported identifiers must have doc comments starting with the name.
- Error strings: lowercase, no trailing punctuation.

### Error Handling

- **Never ignore errors.** If intentionally ignored, add a comment explaining why.
- **Wrap errors with context** using `%w`:
  ```go
  return fmt.Errorf("reading config: %w", err)
  ```
- **Do not panic** for expected errors; return them.
- **Check `Close()` errors** when writing to files or connections.
- **Avoid `log.Fatal` inside libraries**; return errors.
- **SystemError** : IF go-anansi is a dependency of the project. Prefer using `common.SystemError` over other error construction methods.

### Concurrency

- **Always run tests with `-race`.**
- **Ensure goroutines can exit** – avoid leaks by using `context.Context`, `sync.WaitGroup`, or channel closure.
- **Pass `context.Context` as first parameter**, named `ctx`.
- **Never copy locks** (`sync.Mutex`, `sync.WaitGroup`) after use.
- **Prefer channels for coordination, mutexes for shared state.**
- **Clarify channel ownership** – who closes it? Avoid sending on closed channels.

### Performance

- **Avoid premature optimization**; write clear code first.
- **Avoid allocations in hot paths** – use `strings.Builder` instead of `+` in loops.
- **Use value receivers for small immutable types, pointer receivers for large or mutable types.**
- **Do not guess performance** – use benchmarks and `pprof`.

### Security

- **Validate all external input.**
- **Use parameterized queries** to prevent SQL injection.
- **Sanitize file paths** to prevent path traversal.
- **Never log secrets.**
- **Check dependencies for known vulnerabilities** (`govulncheck`).

### Testing

- **Use table‑driven tests** with subtests (`t.Run`).
- **Use `t.Helper()`** in test helpers.
- **Test behavior, not implementation** – prefer public APIs.
- **Use interfaces for mocking** dependencies.
- **Run tests with race detector in CI.**

### Maintainability

- **Keep functions small and single‑purpose.**
- **Comments explain *why*, not *what*.**
- **Avoid duplicate code** but don’t over‑abstract prematurely.
- **Keep packages cohesive** – avoid import cycles.

---

## 4. SOLID Principles in Go Code Review

SOLID helps keep code flexible, testable, and maintainable. In Go, these principles often translate to **small interfaces, composition over inheritance, and dependency injection**.

### S – Single Responsibility Principle (SRP)

> A type, function, or package should have only one reason to change.

**Review questions:**
- Can you describe what this does in one sentence without “and” or “or”?
- Does it mix concerns? e.g., business logic + persistence, validation + formatting.
- Is this file/function too large? (a smell, not proof)
- What events would force this to change? More than one unrelated event → split.

**Example violation:**
```go
func ProcessOrder(order Order) error {
    // validate
    // calculate total
    // save to DB
    // send email
    // log metrics
}
```
→ Split into separate functions/services.

### O – Open/Closed Principle (OCP)

> Open for extension, closed for modification.

**Review questions:**
- Does adding a new behavior require modifying existing code?
- Are there `switch`/`if` chains on concrete types that will grow?
- Can we define an interface and let new types implement it?
- Could a map of strategies replace a type switch?

**Example violation:**
```go
func area(s Shape) float64 {
    switch t := s.(type) {
    case Circle: ...
    case Rectangle: ...
    // adding Triangle requires modifying this function
    }
}
```
→ Prefer an `Area()` method on each shape via an interface.

### L – Liskov Substitution Principle (LSP)

> Subtypes must be substitutable for their base types without altering correctness.

**Review questions:**
- Does an implementation weaken preconditions or strengthen postconditions?
- Does it return errors for inputs the interface would accept?
- Are there type assertions in calling code? (often indicates LSP violation)
- Does the implementation satisfy the interface’s **documented semantics**, not just method signatures?

**Example violation:**
```go
type Store interface { Save([]byte) error }
type ReadOnlyStore struct{}
func (ReadOnlyStore) Save([]byte) error { return errors.New("not supported") }
```
→ Split interfaces: `Reader` and `Writer`.

### I – Interface Segregation Principle (ISP)

> Clients should not depend on methods they do not use.

**Review questions:**
- Are interfaces large or “fat”? Many unrelated methods?
- Do implementations have empty or no‑op methods?
- Are callers using only a subset of an interface?
- Can the interface be split into smaller, role‑specific ones?

**Example violation:**
```go
type Machine interface { Print(); Scan(); Fax() }
type OldPrinter struct{}
func (OldPrinter) Scan() {} // no‑op
func (OldPrinter) Fax()  {} // no‑op
```
→ Separate `Printer`, `Scanner`, `Faxer`.

### D – Dependency Inversion Principle (DIP)

> High‑level modules should not depend on low‑level modules; both should depend on abstractions.

**Review questions:**
- Are high‑level types directly instantiating low‑level details (e.g., `sql.Open` inside a service)?
- Do constructors accept concrete types instead of interfaces?
- Can dependencies be easily mocked for testing?
- Should the dependency be passed in rather than created internally?

**Example violation:**
```go
type UserService struct { db *sql.DB }
func NewUserService() *UserService {
    db, _ := sql.Open("postgres", "...")
    return &UserService{db: db}
}
```
→ Accept an interface like `UserRepository` via constructor injection.

---

## 5. Using Devnotes for Code Review

Devnotes comments act as structured review comments embedded in the code. Because the `note add` command is unavailable, **all notes must be written manually**.

### 5.1 Note Grammar (Manual Writing Cheat Sheet)

A note is a comment block with this **exact shape**:

```
header_line
directive_line*      (@author, @see, or custom @name)
separator_line        <- one blank comment line, MANDATORY if body follows
body_line*            (free text)
```

#### Header Line

```
@note #<id> <category> [field field ...] : <title>
```

- `#<id>` – letters/digits/`_`/`-`. Use a unique ID scheme, e.g., `#review-20260820-001`.
- `<category>` – one of: `observation`, `todo`, `issue`, `context`, `lesson`, `prompt`, or custom (custom produces warnings).
- Fields (any order): `status` (`open`/`resolved`/`wontfix`/`deprecated`), `priority` (`P0`–`P3`), `timestamp` (ISO‑8601), `tags` (`#tag1,#tag2`).
- ` : ` – the first literal space‑colon‑space ends fields and starts title. Everything after is title text.

#### Directives

Must appear **before any body text**. After body begins, `@see`/`@author` are treated as body text.
```go
@see #other-id
@author name
```

#### Separator Line

**One blank comment line** between metadata and body. **Without it, body text is silently discarded.**

#### Body Lines

Free text, byte‑for‑byte. A bare `@note` line inside body is safe; `@note #new-id` starts a new note.

**Important:** Do not place two `@note` headers on directly adjacent lines; separate them with a blank line.

### 5.2 Review Note Categories

Use these categories for code review:

| Category     | Use for                                                      |
|--------------|--------------------------------------------------------------|
| `issue`      | Bugs/defects that must be fixed                               |
| `todo`       | Required follow‑up work (tests, refactoring, documentation)   |
| `observation`| Suggestions, non‑blocking comments, design considerations     |
| `context`    | Explanation left for future maintainers                       |
| `lesson`     | A reusable pattern or anti‑pattern discovered                 |
| `prompt`     | Open question needing discussion                              |

### 5.3 Priority Mapping

- **P0** – Must fix before merge (release blocker)
- **P1** – Should fix before merge (important)
- **P2** – Can be fixed in a follow‑up
- **P3** – Minor / optional

### 5.4 Example of a Manually Written Review Note (Go)

```go
// @note #review-20260820-001 issue status=open priority=P1 tags=#review,#security : SQL injection risk in user query
// @author alice
// @see #context-20260819-005
//
// The query is built using fmt.Sprintf with user input.
// Use parameterized queries (e.g., db.Query with placeholders).
func GetUser(db *sql.DB, username string) (*User, error) {
    query := fmt.Sprintf("SELECT * FROM users WHERE name = '%s'", username)
    // ...
}
```

**After fix**, the author edits the same block:

```go
// @note #review-20260820-001 issue status=resolved priority=P1 tags=#review,#security : SQL injection risk in user query
// @author alice
// @see #context-20260819-005
//
// Fixed by using db.Query with placeholders in commit abc123.
func GetUser(db *sql.DB, username string) (*User, error) {
    query := "SELECT * FROM users WHERE name = ?"
    // ...
}
```

### 5.5 Manual Editing Workflow

1. **Write or edit the note comment directly** in the source file.
2. **Ensure the separator line is present** if body text exists.
3. **Place directives before body.**
4. **Use unique IDs** to avoid `DUPLICATE_ID` errors.
5. **After editing, run:**
   ```bash
   devnotes check
   devnotes index update
   ```
   `devnotes check` will catch missing titles, duplicate IDs, invalid statuses, unresolved references, etc.  
   *Remember:* `check` does **not** detect a missing separator or misplaced directives – human inspection is still needed.

### 5.6 Querying Review Notes

Use these read‑only commands to track review progress:

```bash
# List all open review notes
devnotes list --status=open --tags=#review

# Show a specific note
devnotes show #review-20260820-001

# Trace references
devnotes trace #review-20260820-001

# Generate a report (if available)
devnotes report --tag=#review
```

### 5.7 Common Pitfalls When Writing Notes Manually

| Pitfall | Consequence | How to Avoid |
|---------|-------------|--------------|
| Missing separator line | Body text silently discarded | Always add a blank comment line before body |
| Directives placed after body | `@see` not recognized as reference | Put `@see`/`@author` immediately after header |
| Two `@note` headers on adjacent lines | Second note misparsed as directive of first | Separate notes with a blank line |
| Using the same ID twice | `DUPLICATE_ID` error | Use unique ID scheme with timestamp/counter |
| Forgetting `devnotes index update` after manual edit | Queries return stale data | Always run update after editing |
| Using a colon in title incorrectly | Title truncated at first ` : ` | Avoid ` : ` inside title or escape/quote (title cannot contain that exact sequence) |

---

## 6. Integrated Code Review Workflow

### Step 1: Pre‑Review (Automated)
Run all automated checks (formatting, vet, tests, race). Only proceed if they pass.

### Step 2: Human Review
Examine the diff line by line using the Go checklist and SOLID principles. As you find issues, **write devnotes manually** at the exact code location.

- Use `issue` for bugs, `todo` for required follow‑ups, `observation` for suggestions.
- Assign appropriate priority and tags.
- Add `@see` references to related notes (e.g., context notes, other issues).
- Ensure the note grammar is correct (especially separator).

### Step 3: After Review
Run:
```bash
devnotes check
devnotes index update
```
Fix any diagnostic errors (duplicate IDs, invalid statuses, unresolved references).

Share the list of open review notes with the author:
```bash
devnotes list --status=open --tags=#review
```

### Step 4: Author Resolution
The author addresses each note:
- Makes code changes.
- **Edits the note manually** to change `status=open` to `status=resolved` (or `wontfix`/`deprecated`).
- Adds a brief body describing the fix, if desired.
- Runs `devnotes check` and `devnotes index update`.

### Step 5: Reviewer Verification
The reviewer pulls the changes and runs:
```bash
devnotes list --status=open --tags=#review
```
If no open review notes remain, the review is complete. Optionally, use `devnotes show` to inspect specific resolutions.

---

## 7. Summary Checklist

Use this quick checklist during review:

- [ ] Code compiles and all automated tests pass (including `-race`)
- [ ] Formatting, vet, and lint clean
- [ ] Go idioms followed (naming, error handling, concurrency, etc.)
- [ ] SOLID principles respected:
  - [ ] SRP – each function/type has one reason to change
  - [ ] OCP – new behavior doesn’t require modifying existing code
  - [ ] LSP – implementations honor interface contracts
  - [ ] ISP – interfaces are small and focused
  - [ ] DIP – dependencies depend on abstractions, not concretions
- [ ] Review findings recorded as devnotes at correct locations
- [ ] Note grammar correct (header, directives, separator, body)
- [ ] `devnotes check` passes
- [ ] `devnotes index update` run after manual edits
- [ ] All open review notes resolved before merge

---

By integrating Go best practices, SOLID principles, and structured devnotes, this workflow ensures a thorough, traceable, and maintainable code review process.
