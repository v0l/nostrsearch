import { useCallback, useEffect, useState } from "preact/hooks";
import { ApiError, NO_SIGNER, api, connect, hasSigner, waitForSigner } from "./api";

/**
 * Admin sign-in over NIP-07.
 *
 * There is no session on the node: every admin request carries its own signed
 * NIP-98 event, so "signed in" here means only that the browser has a signer
 * and that the key it holds is on the node's allowlist. Both are checked at
 * sign-in with one real admin call, because the alternative — trusting a
 * remembered pubkey — shows an operator a working console that fails on the
 * first action they take.
 */
export type SessionState =
  | { status: "loading" }
  | { status: "no-signer" }
  | { status: "signed-out" }
  | { status: "checking"; pubkey: string }
  | { status: "denied"; pubkey: string; reason: string }
  | { status: "signed-in"; pubkey: string };

const PUBKEY_KEY = "nostrsearch.admin.pubkey";

export interface Session {
  state: SessionState;
  /** True only when admin requests are expected to succeed. */
  authed: boolean;
  signIn: () => Promise<void>;
  signOut: () => void;
  retry: () => void;
}

/** What a locked panel should say, in the voice of the thing it is missing. */
export function gateMessage(s: SessionState): string {
  switch (s.status) {
    case "loading":
      return "Looking for a signer…";
    case "no-signer":
      return "Install a NIP-07 signer to see admin data.";
    case "checking":
      return "Checking your key with this node…";
    case "denied":
      return "This node does not accept that key for admin data.";
    default:
      return "Sign in with nostr to see admin data.";
  }
}

export function useSession(onError: (msg: string) => void): Session {
  const [state, setState] = useState<SessionState>({ status: "loading" });

  const verify = useCallback(
    async (pubkey: string) => {
      setState({ status: "checking", pubkey });
      try {
        await api.analyses();
        setState({ status: "signed-in", pubkey });
        localStorage.setItem(PUBKEY_KEY, pubkey);
      } catch (e) {
        if (e instanceof ApiError && e.isAuth) {
          setState({ status: "denied", pubkey, reason: e.message });
          return;
        }
        // The node answered, just not about auth (writer restarting, node has
        // no scraper, …). The key is fine; let the panels report the detail.
        setState({ status: "signed-in", pubkey });
        localStorage.setItem(PUBKEY_KEY, pubkey);
      }
    },
    [],
  );

  // Resume a remembered key, but only once a signer is actually available.
  useEffect(() => {
    let alive = true;
    const remembered = localStorage.getItem(PUBKEY_KEY);
    waitForSigner().then((ok) => {
      if (!alive) return;
      if (!ok) {
        setState({ status: "no-signer" });
        return;
      }
      if (remembered) void verify(remembered);
      else setState({ status: "signed-out" });
    });
    return () => {
      alive = false;
    };
  }, [verify]);

  const signIn = useCallback(async () => {
    try {
      const pk = await connect();
      await verify(pk);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg === NO_SIGNER) setState({ status: "no-signer" });
      onError(msg);
    }
  }, [verify, onError]);

  const signOut = useCallback(() => {
    localStorage.removeItem(PUBKEY_KEY);
    setState(hasSigner() ? { status: "signed-out" } : { status: "no-signer" });
  }, []);

  const retry = useCallback(() => {
    if (state.status === "denied" || state.status === "signed-in") void verify(state.pubkey);
    else void signIn();
  }, [state, verify, signIn]);

  return { state, authed: state.status === "signed-in", signIn, signOut, retry };
}
