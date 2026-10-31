# Reproducible fault scenarios

Each file here is a scenario manifest in the shape of design section 12.2: a
seeded dataset, a connector configuration, one named injection, and the
assertions that decide PASS or FAIL.

They are **descriptors, not a second test runner.** Each one names the test
that implements it, so a scenario's behaviour lives in exactly one place and
its parameters are recorded in exactly one place. A manifest whose
`implemented_by` test does not exist describes a scenario that has not been
built — the state the design calls `NOT_TESTED`.

Two rules the manifests exist to keep visible:

- **`fault_activation_expected` is checked.** A run where the injection never
  fired is `NOT_CONCLUSIVE`, never a pass (WF2-P-03).
- **`worker_mode` is part of the scenario.** A case that needs a process kill
  cannot be satisfied in thread mode, and a manifest that omitted the mode
  would let a thread-mode run claim it (WF2-R-04).

Run the suite that implements them, and collect the evidence:

```bash
make whatif        # runs every lane, writes what_if_results.json
make scorecard     # renders the result, per case and per axis
```
