# Contributing

Use focused feature branches and Conventional Commits. Every semantic change needs a test that fails without it. Run `mvn clean verify` on JDK 25 and 27 before opening a PR.

Storage format changes require golden compatibility fixtures, a documented version policy, corruption/truncation tests, and an explicit migration or rejection strategy. Do not introduce preview JDK APIs into the stable artifact. Performance changes require raw benchmark evidence and must not trade away durability or atomicity.
