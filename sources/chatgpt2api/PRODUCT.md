# Product

## Register

product

## Users

Administrators manage the service configuration, account pool, user keys, quotas, logs, and backups. Ordinary key holders use the service APIs and image workflows without access to administrative settings. Administrators typically work in a repeated operational workflow and need to scan state, make changes, and verify outcomes quickly.

## Product Purpose

ChatGPT2API provides a compatibility layer for text, image, edit, search, and related requests while managing upstream accounts and local service state. The product succeeds when requests are routed reliably, failures are diagnosable, quotas are predictable, and administrative work remains fast even as account and request counts grow.

## Brand Personality

Quiet, direct, dependable. The interface should build confidence through clear state, conservative defaults, and immediate feedback rather than decoration or marketing language.

## Anti-references

Avoid marketing-style hero layouts, oversized promotional copy, decorative gradients, excessive rounded cards, dense status colors without meaning, and interaction patterns that hide operational state behind unnecessary dialogs or animation.

## Design Principles

- Make operational state scannable before making controls prominent.
- Keep quota, scheduler, and request behavior explicit and auditable.
- Prefer predictable, reversible actions with clear feedback.
- Use density where it reduces repeated navigation and comparison effort.
- Treat performance as part of the user experience, including loading, polling, and error states.

## Accessibility & Inclusion

Target WCAG 2.1 AA for the administrative interface. Maintain keyboard access, visible focus states, readable contrast, semantic labels, and text alternatives for icon-only controls. Do not rely on color alone for status. Respect reduced-motion preferences and keep polling and loading feedback understandable without animation.
