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
     *  - /api/auth            (NextAuth callbacks — must stay public for login itself to work)
     *  - /login               (sign-in page itself)
     *  - /_next/**            (Next.js internals)
     *  - /favicon.ico
     *
     * api/admin was previously exempted here for local development
     * convenience and never reverted — this left topic create/delete
     * reachable by any unauthenticated client. It is intentionally NOT
     * exempted: admin routes require a valid session like everything else.
     */
    "/((?!api/auth|login|_next/static|_next/image|favicon\\.ico).*)",
  ],
};
