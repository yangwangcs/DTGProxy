# DTGProxy Cypher and Bolt Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan.

**Goal:** Implement versioned Cypher 5/Cypher 25 plus DTG temporal syntax, semantic analysis, compilation, procedures, PackStream, and a transport-independent Bolt state machine connected to the gateway session API.

**Architecture:** Parse into a lossless CST and stable AST, bind/type-check against a catalog, lower to Temporal IR v2, and expose the compiler through one query service. Bolt decoding is pure and bounded; network transports delegate all behavior to the same state machine.

**Tech Stack:** Rust 1.93, Tokio, existing control-plane/gateway crates, blake3, proptest/fuzz targets, no parser generator or unsafe code in the core.

## Global Constraints

- Preserve the existing `temporal-query` DSL and tests.
- Reject unsupported profile syntax during parsing, not execution.
- Retain byte offsets for all tokens, CST nodes, AST nodes, and diagnostics.
- Bound text, tokens, AST depth/node count, parameter size, PackStream nesting, frame size, cursors, and buffered rows.
- Treat authentication as a pluggable interface; never log credentials or raw auth dictionaries.

---

### Task 1: Scaffold value, AST, and syntax crates

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/cypher-ast/Cargo.toml`
- Create: `crates/cypher-ast/src/{lib,version,value,expression,pattern,clause,statement,temporal,visit}.rs`
- Create: `crates/cypher-syntax/Cargo.toml`
- Create: `crates/cypher-syntax/src/{lib,limits,token,scanner,lexer,cst,parser,diagnostic}.rs`
- Test: `crates/cypher-syntax/tests/{version_scanner,lexer,parser_temporal,parser_cypher5,parser_cypher25,limits}.rs`

1. Write failing tests for default Cypher 25, explicit `CYPHER 5`, source spans, escaped identifiers, comments, parameters, standard clauses, `AT VALID_TIME`, `AT TRANSACTION_TIME`, and `DIFF GRAPH`.
2. Run `cargo test -p cypher-syntax`; expect unresolved crate/modules.
3. Implement immutable AST/value types, limits, version scanner, tokens, lexer, Pratt expression parser, clause parser, recovery diagnostics, and CST-to-AST construction.
4. Add table-driven grammar tests for every stable clause listed in specification §3.2 and temporal examples in §4.
5. Run `cargo test -p cypher-syntax`; expect all tests pass.
6. Commit: `feat(cypher): add versioned syntax and temporal AST`.

### Task 2: Semantic catalog, binder, and type checker

**Files:**
- Create: `crates/cypher-sema/Cargo.toml`
- Create: `crates/cypher-sema/src/{lib,catalog,scope,binder,types,value_semantics,aggregate,update,temporal,functions,errors}.rs`
- Test: `crates/cypher-sema/tests/{scope,types,null_logic,aggregation,updates,paths,temporal,procedures}.rs`

1. Write failing tests for variable shadowing, WITH scope, UNION shape, aggregate grouping, three-valued predicates, numeric overflow, path uniqueness, write ordering, temporal scope inheritance, and attempts to assign transaction time.
2. Run `cargo test -p cypher-sema`; expect failure because semantic APIs do not exist.
3. Implement `CompilationCatalog`, symbol/slot tables, type lattice, function overload resolution, aggregate validation, update-effect validation, and temporal scope resolution.
4. Make each error return a stable diagnostic code and exact source span.
5. Run `cargo test -p cypher-sema`; expect all tests pass.
6. Commit: `feat(cypher): bind and type-check versioned queries`.

### Task 3: Compiler, normalization, fingerprint, and cache

**Files:**
- Create: `crates/cypher-compiler/Cargo.toml`
- Create: `crates/cypher-compiler/src/{lib,options,normalize,lower,fingerprint,cache,legacy}.rs`
- Test: `crates/cypher-compiler/tests/{compile,normalize,lower_temporal,cache_key,legacy_equivalence}.rs`
- Modify: `crates/temporal-ir/src/lib.rs`

1. Write failing tests that assert AST normalization, profile-specific fingerprints, parameter schemas, result schemas, effect classification, and initial IR v2 nodes.
2. Run `cargo test -p cypher-compiler`; expect failure.
3. Implement `CypherCompiler::compile`, deterministic normalization, plan-cache keying, and lowering for all AST operator families.
4. Implement `LegacyPlanAdapter` and prove equivalence for all current v1 query tests.
5. Run `cargo test -p cypher-compiler`; expect pass.
6. Commit: `feat(cypher): compile semantic AST into temporal IR v2`.

### Task 4: Procedure runtime

**Files:**
- Create: `crates/procedure-runtime/Cargo.toml`
- Create: `crates/procedure-runtime/src/{lib,catalog,descriptor,native,remote,wasm,limits,error}.rs`
- Test: `crates/procedure-runtime/tests/{catalog,signature,permissions,limits,cancel}.rs`

1. Write failing tests for duplicate registration, typed arguments/results, language-profile gates, deterministic flags, read/write effects, permission denial, timeout, row/byte limits, and cancellation.
2. Implement stable `ProcedureDescriptorV1`, catalog snapshots, native Rust provider, remote-provider trait, and a WASM host contract that rejects execution until a configured runtime is installed.
3. Register DTG status, plan-cache, projection, and analytics procedures without Neo4j plugin ABI assumptions.
4. Run `cargo test -p procedure-runtime`; expect pass.
5. Commit: `feat(procedure): add bounded typed procedure runtime`.

### Task 5: PackStream and Bolt messages

**Files:**
- Create: `crates/bolt-protocol/Cargo.toml`
- Create: `crates/bolt-protocol/src/{lib,limits,handshake,manifest,packstream,value,message,codec,state,error}.rs`
- Test: `crates/bolt-protocol/tests/{handshake,manifest,packstream_vectors,messages,malformed,limits}.rs`
- Test: `crates/bolt-protocol/fuzz/fuzz_targets/{packstream,framing}.rs`

1. Write official-vector and malformed-input tests before codecs.
2. Run `cargo test -p bolt-protocol`; expect failure.
3. Implement bounded big-endian framing, PackStream primitives/structures, graph/spatial/temporal values, version negotiation, manifests, and messages for HELLO/LOGON/LOGOFF/GOODBYE/RUN/PULL/DISCARD/RESET/INTERRUPT/BEGIN/COMMIT/ROLLBACK/ROUTE.
4. Ensure decode never panics and never allocates before checking declared size/depth.
5. Run unit/property/fuzz smoke tests; expect pass.
6. Commit: `feat(bolt): implement bounded PackStream and protocol messages`.

### Task 6: Gateway session service and Bolt machine

**Files:**
- Modify: `crates/gateway-node/src/lib.rs`
- Create: `crates/gateway-node/src/{session,cursor,auth,bookmark}.rs`
- Create: `crates/bolt-server/Cargo.toml`
- Create: `crates/bolt-server/src/{lib,machine,connection,tcp,tls,websocket,routing,config,error}.rs`
- Test: `crates/bolt-server/tests/{state_machine,autocommit,explicit_tx,pull_discard,reset,route,backpressure}.rs`

1. Write state-transition tests covering valid and invalid messages in every connection state.
2. Implement `QueryService`, session registry, cursor paging, bookmark codec, authentication trait, and exact cleanup rules.
3. Implement `BoltMachine<S>` and map diagnostics/runtime failures to stable Neo4j-compatible error classifications plus DTG metadata.
4. Add TCP transport; add TLS/WebSocket adapters behind features while keeping the machine identical.
5. Run `cargo test -p gateway-node -p bolt-server`; expect pass.
6. Commit: `feat(bolt): connect transactional state machine to gateway sessions`.

### Task 7: Compatibility and integration matrix

**Files:**
- Create: `tests/cypher_compatibility.rs`
- Create: `tests/bolt_driver_compatibility.rs`
- Create: `tests/temporal_cypher_e2e.rs`
- Modify: `crates/gateway-node/src/service.rs`
- Modify: `crates/dtgproxy/src/gateway.rs`

1. Route query text through `CypherCompiler`; keep explicit debug DSL selection for legacy operations.
2. Run Cypher 5/Cypher 25/DTG temporal positive and negative matrices.
3. Exercise auto-commit and explicit transactions through the same session service using Bolt message sequences.
4. Run official driver smoke clients where available and record protocol versions tested.
5. Run `cargo test -p cypher-syntax -p cypher-sema -p cypher-compiler -p bolt-protocol -p bolt-server -p gateway-node`; expect pass.
6. Commit: `feat(gateway): expose temporal Cypher through Bolt and gateway API`.
