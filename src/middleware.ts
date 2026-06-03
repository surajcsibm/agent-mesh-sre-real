// Route protection — requires a valid Google session for all pages.
// Uses withAuth for explicit token checking (works correctly on Vercel Edge).

import { withAuth } from "next-auth/middleware";

export default withAuth({
  callbacks: {
    authorized({ token }) {
      // !!token = logged in; null token = redirect to /login
      return !!token;
    },
  },
  pages: {
    signIn: "/login",
  },
});

export const config = {
  matcher: [
    /*
     * Protect everything EXCEPT:
     *  - /api/auth|api/admin/**         (NextAuth callbacks)
     *  - /login               (sign-in page itself)
     *  - /_next/**            (Next.js internals)
     *  - /favicon.ico
     */
    "/((?!api/auth|api/admin|login|_next/static|_next/image|favicon\\.ico).*)",
  ],
};
