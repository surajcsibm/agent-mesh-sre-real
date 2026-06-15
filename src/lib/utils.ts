import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}

export function relTime(ts: number): string {
  const diff = (Date.now() - ts) / 1000;
  if (diff < 1) return "just now";
  if (diff < 60) return `${diff.toFixed(0)}s ago`;
  if (diff < 3_600) return `${(diff / 60).toFixed(0)}m ago`;
  return `${(diff / 3600).toFixed(1)}h ago`;
}
// cache bust Mon Jun 15 18:16:41 IST 2026
