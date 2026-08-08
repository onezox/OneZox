import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'OneZox Admin',
  description: 'OneZox operator console — models, rollouts, keys, audit.',
  // An internal operator console has no business being indexed, and it
  // is reachable over TLS from a browser like anything else.
  robots: { index: false, follow: false },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
