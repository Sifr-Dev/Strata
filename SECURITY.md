# Security and operational boundaries

Strata provides no encryption, authentication or network protocol. Protect the database directory with operating-system permissions. CRC-32 detects accidental corruption, not malicious modification. A malicious writer with file access can replace both data and checksums.

Only one process may open a directory for writing. The file lease is a coordination mechanism, not a sandbox or defense against a process that ignores it. Do not open databases on filesystems without the required atomic-replace and synchronization behavior.

Public API limits bound individual keys, values and live payload bytes; Java object overhead, transient copy-on-write memory, and full-state compaction require additional operational headroom. There are no transactions beyond an atomic `write` batch, no SQL interface and no cross-process shared state.

Report reproducible integrity or resource-safety issues through a private GitHub security advisory rather than embedding secrets in an issue.
