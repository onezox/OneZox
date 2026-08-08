import { redirect } from 'next/navigation';

/**
 * The panel has no distinct landing page — an operator opening it wants
 * the dashboard. The (panel) layout handles the "not signed in" case by
 * redirecting on to /login, so this stays a single unconditional hop.
 */
export default function RootPage() {
  redirect('/dashboard');
}
