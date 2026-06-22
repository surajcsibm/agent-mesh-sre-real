import { NextResponse } from "next/server";
import { getRuntime } from "@/lib/runtime-mode";
import { listConsumerGroups, describeConsumerGroup } from "@/lib/kafka-admin-cfk";
import { safeErr } from "@/lib/log-safe";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  const rt = getRuntime();
  const { searchParams } = new URL(req.url);
  const groupId = searchParams.get("groupId");

  if (rt.mode !== "real") {
    return NextResponse.json({ mode: "MOCK", groups: [] });
  }

  try {
    if (groupId) {
      const detail = await describeConsumerGroup(groupId);
      return NextResponse.json({ mode: "REAL", group: detail });
    }
    const groups = await listConsumerGroups();
    return NextResponse.json({ mode: "REAL", groups });
  } catch (e) {
    return NextResponse.json(
      { error: safeErr(e).message },
      { status: 500 }
    );
  }
}