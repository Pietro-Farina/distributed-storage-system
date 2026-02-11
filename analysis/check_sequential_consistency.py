import json
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import defaultdict
import re
from typing import List, Tuple, Optional

"""
Sequential Consistency Checker for Distributed Storage System

This checker validates Sequential Consistency (SC), NOT Linearizability.

Key difference:
- Linearizability: Must respect real-time ordering (timestamps)
- Sequential Consistency: Only requires a valid total ordering that preserves
  per-client program order, regardless of wall-clock time

Example: If client A writes x=1 at t=1.0 and client B reads x=0 at t=2.0,
  Linearizability: VIOLATION (B must see A's write)
  Sequential Consistency: OK (can order as: Read-B → Write-A)

Checks performed:
1. Read-your-writes: Each client must see its own writes
2. Monotonic reads: Same client can't see older versions after newer ones
3. No impossible reads: Can't read versions that were never written
4. Version consistency: No missing version numbers (lost updates)
"""


# --------- parsing helpers ---------

REQID_RE = re.compile(r"^(?P<client>\d+)-(?P<seq>\d+)$")

def parse_ts_iso_z(s: str) -> int:
    """
    Parse timestamps like: 2026-01-22T18:02:31.615350400Z
    Return integer nanoseconds since epoch (best-effort).
    Python datetime supports microseconds; we'll keep microsecond precision.
    """
    # trim to microseconds to avoid ValueError (615350400 -> 615350)
    if s.endswith("Z"):
        s2 = s[:-1]
    else:
        s2 = s

    # split fractional seconds
    if "." in s2:
        base, frac = s2.split(".", 1)
        frac = (frac + "000000")[:6]  # microseconds
        s2 = f"{base}.{frac}"
    dt = datetime.fromisoformat(s2).replace(tzinfo=timezone.utc)
    # microsecond precision
    return int(dt.timestamp() * 1_000_000_000)


def parse_reqid(reqid: str) -> Optional[Tuple[int, int]]:
    """
    Return (client_id, seq_no) if format matches '10-2', else None
    """
    m = REQID_RE.match(reqid)
    if not m:
        return None
    return int(m.group("client")), int(m.group("seq"))


@dataclass(frozen=True)
class LogEvent:
    ts_ns: int
    node_id: int
    req_id: str
    op: str
    key: int
    version: int
    phase: str
    success: bool


def load_jsonl(path: str) -> List[LogEvent]:
    events = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            events.append(LogEvent(
                ts_ns=parse_ts_iso_z(obj["ts"]),
                node_id=int(obj["nodeId"]),
                req_id=str(obj["reqId"]),
                op=str(obj["operation"]),
                key=int(obj["key"]),
                version=int(obj["version"]),
                phase=str(obj["phase"]),
                success=bool(obj["success"]),
            ))
    return events


# --------- Sequential Consistency checks (program-order based) ---------

def check_sequential_consistency(events: List[LogEvent], W: int = 2) -> List[str]:
    """
    Check necessary conditions for Sequential Consistency:
    
    Sequential Consistency requires there exists SOME total ordering where:
    1. Each client's operations appear in program order (by sequence number)
    2. Each read returns the value from the most recent write in that ordering
    
    Unlike linearizability, SC does NOT require respecting real-time ordering
    between different clients.
    
    We check necessary conditions:
    - Read-your-writes: If a client writes then reads, must see own write (or later)
    - Monotonic reads: Within same client, version numbers shouldn't go backwards
    - No impossible reads: Can't read a version that was never successfully written
    """
    errors = []

    # Build a mapping of successful operations per client
    # client -> list of (seq, op_type, key, version, req_id)
    client_ops = defaultdict(list)
    
    # Track all successfully written versions: key -> set of versions
    written_versions = defaultdict(set)
    
    # Collect all successful operations
    for e in events:
        if not e.success:
            continue
            
        if e.op == "UPDATE" and e.phase == "DEC_UPDATE_SUCCESS":
            parsed = parse_reqid(e.req_id)
            if parsed:
                client_id, seq = parsed
                client_ops[client_id].append((seq, "UPDATE", e.key, e.version, e.req_id))
                written_versions[e.key].add(e.version)
                
        elif e.op == "GET" and e.phase == "DEC_GET_SUCCESS":
            parsed = parse_reqid(e.req_id)
            if parsed:
                client_id, seq = parsed
                client_ops[client_id].append((seq, "GET", e.key, e.version, e.req_id))
    
    # Sort each client's operations by sequence number
    for client_id in client_ops:
        client_ops[client_id].sort(key=lambda x: x[0])
    
    # Check 1: Read-your-writes within each client
    for client_id, ops in client_ops.items():
        last_write_per_key = {}  # key -> (seq, version)
        
        for seq, op_type, key, version, req_id in ops:
            if op_type == "UPDATE":
                last_write_per_key[key] = (seq, version)
            elif op_type == "GET":
                # Check if this client previously wrote to this key
                if key in last_write_per_key:
                    write_seq, write_version = last_write_per_key[key]
                    # Read comes after write in program order
                    if seq > write_seq and version < write_version:
                        errors.append(
                            f"[READ-YOUR-WRITES] client={client_id} wrote v={write_version} "
                            f"(seq={write_seq}) but later read v={version} (seq={seq}) for key={key}"
                        )
    
    # Check 2: Monotonic reads within each client
    for client_id, ops in client_ops.items():
        last_read_per_key = {}  # key -> (seq, version)
        
        for seq, op_type, key, version, req_id in ops:
            if op_type == "GET":
                if key in last_read_per_key:
                    prev_seq, prev_version = last_read_per_key[key]
                    if version < prev_version:
                        errors.append(
                            f"[MONOTONIC-READS] client={client_id} read v={prev_version} "
                            f"(seq={prev_seq}) but later read v={version} (seq={seq}) for key={key}"
                        )
                last_read_per_key[key] = (seq, version)
    
    # Check 3: No impossible reads (reading versions that were never written)
    for client_id, ops in client_ops.items():
        for seq, op_type, key, version, req_id in ops:
            if op_type == "GET" and version > 0:
                if version not in written_versions[key]:
                    errors.append(
                        f"[IMPOSSIBLE-READ] client={client_id} read v={version} for key={key} "
                        f"but that version was never successfully written"
                    )
    
    # Check 4: Version consistency - no gaps that violate causality
    # For each key, versions should be written in order (allowing for concurrent writes)
    for key, versions in written_versions.items():
        if not versions:
            continue
        max_version = max(versions)
        # Check if all versions from 1 to max exist
        for v in range(1, max_version + 1):
            if v not in versions:
                errors.append(
                    f"[MISSING-VERSION] key={key} has v={max_version} but v={v} was never written "
                    f"(possible lost update or coordinator inconsistency)"
                )

    return errors


def check_write_write_conflicts(events: List[LogEvent]) -> List[str]:
    """
    Detect Write-Write conflicts:
    - Multiple UPDATEs claiming the same version for a key (duplicate versions)
    - Overlapping UPDATEs that might cause lost updates
    
    For SC, we check:
    1. Duplicate versions: Same (key, version) written by different requests
    2. Version ordering: Ensure writes don't create inconsistent version sequences
    """
    errors = []
    
    # Track which request wrote which version: (key, version) -> list of req_ids
    version_writers = defaultdict(list)
    
    # Track UPDATE timing for overlap detection: req_id -> (key, version, start_ts, end_ts)
    update_timing = {}
    
    for e in events:
        if e.op == "UPDATE":
            if e.phase == "INT_UPDATE_START":
                if e.req_id not in update_timing:
                    update_timing[e.req_id] = {'key': e.key, 'start': e.ts_ns, 'end': None, 'version': None}
            elif e.phase == "DEC_UPDATE_SUCCESS" and e.success:
                if e.req_id in update_timing:
                    update_timing[e.req_id]['end'] = e.ts_ns
                    update_timing[e.req_id]['version'] = e.version
                version_writers[(e.key, e.version)].append(e.req_id)
    
    # Check 1: Duplicate versions (multiple requests wrote same version)
    for (key, version), req_ids in version_writers.items():
        if len(req_ids) > 1:
            errors.append(
                f"[WRITE-WRITE-CONFLICT] key={key} v={version} written by multiple requests: "
                f"{', '.join(req_ids)} (lost update or version collision)"
            )
    
    # Check 2: Overlapping writes that produced consecutive versions
    # Group updates by key
    # updates_by_key = defaultdict(list)
    # for req_id, info in update_timing.items():
    #     if info['end'] is not None and info['version'] is not None:
    #         updates_by_key[info['key']].append((info['start'], info['end'], info['version'], req_id))
    
    # for key, updates in updates_by_key.items():
    #     # Sort by version
    #     updates.sort(key=lambda x: x[2])
        
    #     # Check if consecutive version UPDATEs overlapped in time
    #     for i in range(len(updates) - 1):
    #         start1, end1, ver1, req1 = updates[i]
    #         start2, end2, ver2, req2 = updates[i + 1]
            
    #         # Check if they're consecutive versions
    #         if ver2 == ver1 + 1:
    #             # Check for temporal overlap: does req2 start before req1 ends?
    #             if start2 < end1:
    #                 errors.append(
    #                     f"[WRITE-WRITE-OVERLAP] key={key}: {req2} (v={ver2}) started before "
    #                     f"{req1} (v={ver1}) completed - potential race condition"
    #                 )
    
    return errors


def check_write_read_conflicts(events: List[LogEvent]) -> List[str]:
    """
    Detect Write-Read conflicts:
    - GETs that read during an overlapping UPDATE might see inconsistent state
    - Reads that happen during a write should either see old or new value, not partial
    
    For SC, we check if reads during concurrent writes see consistent versions.
    """
    errors = []
    
    # Track UPDATE windows: req_id -> (key, version, start_ts, end_ts)
    update_windows = {}
    for e in events:
        if e.op == "UPDATE":
            if e.phase == "INT_UPDATE_START":
                if e.req_id not in update_windows:
                    update_windows[e.req_id] = {'key': e.key, 'start': e.ts_ns, 'end': None, 'version': None}
            elif e.phase == "DEC_UPDATE_SUCCESS" and e.success:
                if e.req_id in update_windows:
                    update_windows[e.req_id]['end'] = e.ts_ns
                    update_windows[e.req_id]['version'] = e.version
    
    # Track GET timing: req_id -> (key, read_version, start_ts, end_ts)
    get_timing = {}
    for e in events:
        if e.op == "GET":
            if e.phase == "INT_GET_START":
                if e.req_id not in get_timing:
                    get_timing[e.req_id] = {'key': e.key, 'start': e.ts_ns, 'end': None, 'version': None}
            elif e.phase == "DEC_GET_SUCCESS" and e.success:
                if e.req_id in get_timing:
                    get_timing[e.req_id]['end'] = e.ts_ns
                    get_timing[e.req_id]['version'] = e.version
    
    # Check for overlapping READ during UPDATE
    for get_req, get_info in get_timing.items():
        if get_info['end'] is None or get_info['version'] is None:
            continue
            
        key = get_info['key']
        get_start = get_info['start']
        get_end = get_info['end']
        read_version = get_info['version']
        
        # Find all UPDATEs to same key that overlap with this GET
        overlapping_updates = []
        for upd_req, upd_info in update_windows.items():
            if upd_info['end'] is None or upd_info['version'] is None:
                continue
            if upd_info['key'] != key:
                continue
                
            # Check for temporal overlap
            # Overlap if: update_start < get_end AND get_start < update_end
            if upd_info['start'] < get_end and get_start < upd_info['end']:
                overlapping_updates.append((upd_req, upd_info['version']))
        
        # If GET overlapped with UPDATEs, check for consistency
        if overlapping_updates:
            # The read should be either:
            # - The version just before any overlapping update
            # - The version of one of the overlapping updates
            update_versions = [v for _, v in overlapping_updates]
            min_update_version = min(update_versions)
            max_update_version = max(update_versions)
            
            # Read is OK if it's (min-1) or within [min, max]
            expected_range = list(range(min_update_version - 1 if min_update_version > 0 else 0, 
                                       max_update_version + 1))
            
            if read_version not in expected_range:
                update_strs = [f"{req}(v={v})" for req, v in overlapping_updates]
                errors.append(
                    f"[WRITE-READ-CONFLICT] {get_req} read v={read_version} for key={key} "
                    f"during overlapping UPDATEs {', '.join(update_strs)} - "
                    f"expected v in {expected_range}"
                )
    
    return errors


def check_per_client_program_order(events: List[LogEvent]) -> List[str]:
    """
    If reqId encodes program order (client-seq), check that DEC events respect it:
    For each client, seq numbers must appear in nondecreasing order in DEC_*_SUCCESS events.
    This is not required by SC unless you want to enforce client-issued order explicitly.
    """
    errors = []

    dec = [e for e in events if e.success and e.phase.startswith("DEC_") and e.op in ("UPDATE", "GET")]
    dec.sort(key=lambda e: (e.ts_ns, e.req_id, e.node_id))

    last_seq = {}  # client_id -> last_seq_seen
    for e in dec:
        parsed = parse_reqid(e.req_id)
        if not parsed:
            continue
        client, seq = parsed
        prev = last_seq.get(client)
        if prev is not None and seq < prev:
            errors.append(
                f"[PROG-ORDER] client={client} saw reqId={e.req_id} after seq={prev} "
                f"(timestamp order violates client program order assumption)"
            )
        last_seq[client] = max(prev, seq) if prev is not None else seq

    return errors


def run_all_checks(path: str) -> int:
    events = load_jsonl(path)

    errors = []
    errors += check_sequential_consistency(events)
    errors += check_write_write_conflicts(events)
    errors += check_write_read_conflicts(events)

    if not errors:
        print("✅ Sequential Consistency checks passed!")
        print("   - Read-your-writes: OK")
        print("   - Monotonic reads: OK")
        print("   - No impossible reads: OK")
        print("   - Version consistency: OK")
        print("   - Write-write conflicts: OK")
        print("   - Write-read conflicts: OK")
        return 0

    print("❌ Sequential Consistency violations found:")
    
    # Group errors by type for better readability
    sc_errors = [e for e in errors if any(t in e for t in ["READ-YOUR-WRITES", "MONOTONIC-READS", "IMPOSSIBLE-READ", "MISSING-VERSION"])]
    ww_errors = [e for e in errors if "WRITE-WRITE" in e]
    wr_errors = [e for e in errors if "WRITE-READ" in e]
    
    if sc_errors:
        print("\n  📋 Basic SC violations:")
        for err in sc_errors:
            print("    -", err)
    
    if ww_errors:
        print("\n  ⚠️  Write-Write conflicts:")
        for err in ww_errors:
            print("    -", err)
        print("\n     Note: WRITE-WRITE-OVERLAP means concurrent UPDATEs from different coordinators")
        # print("           raced. This is OK for SC if replica-level locking prevents actual conflicts.")
    
    # if wr_errors:
    #     print("\n  ⚠️  Write-Read conflicts:")
    #     for err in wr_errors:
    #         print("    -", err)
    #     print("\n     Note: WRITE-READ-CONFLICT means a GET read during a concurrent UPDATE.")
        # print("           This is OK for SC if the read saw a consistent version.")
    
    print(f"\n  Total violations: {len(errors)}")
    return 1


if __name__ == "__main__":
    # change this to your file name
    exit(run_all_checks("events copy.jsonl"))
