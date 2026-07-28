# Archived Flattener

This directory contains the original generic JSON flattener that was
previously used during development of the Bronze ingestion package.

## Why is it archived?

The Bronze ingestion framework was simplified to preserve nested JSON
structures rather than flattening them automatically. This reduced
complexity, improved schema fidelity, and aligned the package with the
intended Bronze-layer responsibility of storing raw source data.

The flattener implementation is intentionally retained because it is
expected to be useful for future Silver-layer transformations where
business-friendly flattened schemas are required.

## Contents

- `flattener.py`
  - Generic recursive JSON/DataFrame flattening utilities.

- `test_flattener.py`
  - Original unit tests preserved alongside the implementation.

## Status

This code is **not used by the current Bronze ingestion pipeline**.

It is retained as reference and as a reusable starting point for future
Silver-layer processing rather than being deleted.

Keeping the implementation and its tests together makes it easier to
reintroduce or adapt the logic when Silver-layer development begins.