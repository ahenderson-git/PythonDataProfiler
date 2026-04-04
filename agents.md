# Agent Rules (Global)

## Mission
Help improve this Python project safely and predictably:
- Make small, targeted, non-breaking changes by default
- Preserve behaviour unless explicitly asked to change it
- Prefer clarity and correctness over cleverness
- Ask when unsure instead of guessing

---

## Prime Directive (Non-Breaking Contract)
1. Default to **non-breaking changes**
2. Default to **minimal edits**
3. If unsure, **STOP and ask**
4. Do not change behaviour silently
5. Do not "clean up" unrelated code

---

## Clarification Gate (Mandatory)
Ask up to 3 short questions before coding when:
- Requirements are ambiguous
- Multiple valid approaches exist
- Behaviour, performance, or data correctness may change
- The change touches many files (>10)
- A refactor is implied but not requested

If blocked:
- Propose a safe default
- Ask for confirmation

---

## Scope Control
- Make the **smallest change that works**
- Avoid drive-by edits in unrelated files
- Break large tasks into **safe, testable steps**
- Keep each step independently runnable

Before editing, state:
- Intent
- Files to change
- Risks
- How to verify

---

## How to Work
1. Understand current behaviour
2. Identify minimal change
3. Implement clearly (not cleverly)
4. Add or adjust tests if they exist
5. Run checks
6. Provide simple manual test steps

Avoid:
- large rewrites
- unnecessary abstractions
- architectural changes unless asked

---

## Python Principles

### Readability First
- Prefer simple, explicit code
- Avoid clever one-liners that hurt clarity
- Follow the spirit of "explicit is better than implicit"

---

### Errors & Exceptions
- Do not silently swallow errors
- Avoid bare `except:`
- Catch specific exceptions where possible
- Either handle meaningfully or let it fail clearly

---

### Functions & State
- Keep functions small and focused
- Avoid hidden side effects
- Be explicit when mutating inputs or shared state
- Avoid mutable default arguments

---

### Typing (Use When Helpful)
- Add type hints for public functions where useful
- Don't overcomplicate typing
- Keep consistent with the existing codebase

---

### Data Safety
Be careful with:
- dict access (`.get()` vs direct indexing)
- `None` handling
- external inputs (APIs, files, user input)

Never assume structure without checking

---

### Async / Concurrency
When touching:
- `asyncio`
- threads
- multiprocessing

Ensure:
- no blocking calls in async code
- no unsafe shared state
- tasks are properly awaited or managed

If unsure → ask

---

## Compatibility Rules
Do NOT change without approval:
- Public function signatures
- CLI behaviour
- File formats / output structure
- Config structure
- API request/response shapes
- Import paths used elsewhere

If a breaking change is needed:
- explain why
- propose a migration path

---

## Project Structure Guidelines
- Keep modules focused
- Avoid circular imports
- Avoid heavy logic at import time
- Separate core logic from UI/CLI where practical

---

## Dependencies
- Prefer standard library first
- Only add dependencies if justified
- Follow existing dependency management approach
- Do not introduce new frameworks without approval

---

## Testing & Verification

If tests exist:
```bash
pytest
```

If linting/formatting exists:
```bash
ruff check .
black --check .
```

If typing exists:
```bash
mypy .
```

If no tests exist:
- suggest adding minimal tests before major changes

---

## Logging
- Use `logging` (not `print`) for non-trivial code
- Do not log sensitive data
- Keep logs useful, not noisy

---

## Performance & Correctness
- Prefer correctness over optimisation
- Don't optimise without reason
- Avoid unnecessary data copying
- Be mindful of large data loads

---

## External Interactions
When dealing with:
- files
- databases
- APIs
- subprocesses

Ensure:
- errors are handled
- resources are cleaned up (`with` where possible)
- no unsafe command execution

---

## Code Style
Follow existing project style. If none:

- variables/functions → `snake_case`
- classes → `PascalCase`
- constants → `UPPER_SNAKE_CASE`

General:
- keep functions small
- comment non-obvious logic
- prefer clarity over brevity

---

## Output Format (Always)
When proposing changes:

1. Intent
2. Files affected
3. Risks
4. How to verify
5. Manual test steps
6. Next step

---

## Forbidden (Unless Asked)
- Large rewrites
- Renaming for aesthetics
- Removing code without proof it's unused
- Adding heavy dependencies
- Changing behaviour silently
- Mixing refactors with feature work

---

## When In Doubt
- STOP
- Ask questions
- Offer safe options
- Recommend lowest-risk approach

---

## Notes for AI Assistants
- This is a Python project — favour simplicity
- Prioritise correctness and clarity
- Avoid over-engineering
- Ask before introducing new patterns or frameworks
