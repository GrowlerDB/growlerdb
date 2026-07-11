// A tiny path-based router. The Engine serves `index.html` for any non-API path (SPA fallback), so
// client routes are real paths.
import { writable } from 'svelte/store';

export const routes = ['/', '/indexes', '/observability', '/settings'] as const;
export type Route = (typeof routes)[number];

function normalize(pathname: string): Route {
  // The Cluster screen folds into the header Health pill + Observability, and the Ingestion screen
  // into Observability's Ingestion section, so bookmarked `/cluster` and `/ingestion` both redirect
  // there.
  if (pathname === '/cluster' || pathname === '/ingestion') return '/observability';
  return (routes as readonly string[]).includes(pathname) ? (pathname as Route) : '/';
}

/** The current route, reactive. */
export const path = writable<Route>(normalize(window.location.pathname));

/** Navigate to `to` (pushes history, updates the store). */
export function navigate(to: Route): void {
  if (window.location.pathname !== to) {
    window.history.pushState({}, '', to);
  }
  path.set(to);
}

window.addEventListener('popstate', () => path.set(normalize(window.location.pathname)));
