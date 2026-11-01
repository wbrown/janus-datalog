# A since view reads the datoms written after a point

## 1. Goals

1. A query reads the datoms a database wrote after a point, as a source it joins with others.
2. The read costs the datoms written after the point, never the store.
3. Each datom the view yields says whether it asserts or withdraws.
4. The view takes Datomic's meaning: Datomic's `since` of a history database (goal 12 of `NESTED_SCOPES_AS_QUERIES.md`).

## 2. Tests

- `d.History().Since(P)` answers every datom whose transaction is past P and no other, for each cardinality, on the memory-tree and Badger stores.
- A pattern that binds nothing on the view reads TAEV down to P and no further, as a scan count shows.
- A pattern with a bound attribute or value on the view answers the datoms past P that carry it.
- The view passed as `$since` joins with `$` in one query.
- `[$since ?e ?a ?v ?tx false]` answers the withdrawals past P (`../bugs/BUG_HISTORY_CANNOT_TELL_A_WITHDRAWAL_FROM_AN_ASSERTION.md`).
- After a commit that changes nothing, the view of the database past that commit's predecessor's point answers no datom (`../bugs/BUG_AN_EMPTY_COMMIT_WRITES_A_COMMIT_MARK.md`).
- `Since` on a latest or an `AsOf` handle is refused, naming the handle's view.

## 3. What exists

- `PatternMatcher.chooseIndex` (`datalog/storage/matcher.go`) reads TAEV only when the pattern binds its transaction to one ElementID, as a prefix. A pattern that binds nothing scans all of EATV. So `[?e ?a ?v ?tx] [(> ?tx ?p)]` filters a scan of the whole store.
- `MemoryTreeStore.DatomsAfter` and `BadgerStore.DatomsAfter` walk TAEV from the newest datom and stop at the first one not after the point. Each collects the datoms into a slice for `TruncateTo`.
- `PatternMatcher.shouldFilterTx` filters a datom only in as-of mode, when it lies after the as-of point. A History handle filters nothing.
- A database passed as a named source answers patterns on that source (`docs/reference/SCHEMA.md`).

## 4. The view

- `History().Since(P)` returns a History handle that also carries the point P. The handle holds the point as a field beside its mode, as an `AsOf` handle holds its as-of point.
- In history mode with a since point, `shouldFilterTx` filters every datom at or before P.
- Every pattern on the view reads TAEV from the newest datom down to P, and stops at the first datom not after P, as `DatomsAfter` does. The scan streams, and the pattern's bound positions filter the datoms it yields.
- Every datom that scan yields lies in the view, so the read touches no datom the view excludes. Its cost is the datoms past P.
- The alternative is each pattern's own index with the filter at P. It reads the pattern's whole extent at every point, the datoms the view excludes among them.
- A second alternative is `DatomsAfter`'s slice as the view's source. It copies the delta before any pattern has chosen what it reads.
- P is any ElementID, such as the one `Commit` returns or the point a fork holds its parent's state as of.
- `Since` on a latest or an `AsOf` handle is refused. The uses read operations, which only History holds. A current-view since would need one datom behind each current fact, and a vector element has none.
- A `With` value's `History` takes `Since` as any History handle does. The value's overlay orders after everything in its base, so at the base's newest point the view holds the speculation's writes alone (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §3.1).

## 5. What the view holds

- The view holds every datom written after P, the engine's own included.
- A commit's `:db/txInstant` mark is a datom, so each commit past P appears in the view by its mark. `TRANSACTION_ENVELOPES.md` moves the mark off the datoms.
- A commit that holds nothing writes a mark today, which is no fact. `BUG_AN_EMPTY_COMMIT_WRITES_A_COMMIT_MARK.md` removes it. After that fix, a database that changed nothing past P has an empty view.
- A snapshot taken past P writes its `:db.snapshot/*` marker, which is a fact and appears in the view. A reader that asks whether its own facts changed reads the view before it takes the snapshot.

## 6. The uses

A fixpoint whose iterations run outside the engine computes each iteration from the last and stops when nothing changes. Its iterations run in one database, and each commits. The point `Commit` returns names that iteration's state, which `AsOf` reads, so an iteration needs no fork and no named snapshot. A speculative run, one that is read and thrown away, is a `With` value that is never committed, and its `$since` is `d.With(tx).History().Since(P)`. In the queries below, P is the point the previous iteration committed at, or for a speculative run the point the base's last commit returned:

- **Convergence:** the view is empty.

  ```clojure
  [:find ?e . :in $ $since :where [$since ?e ?a ?v]]
  ```

- **Withdrawals,** which a least fixpoint forbids:

  ```clojure
  [:find ?e ?a ?v :in $ $since :where [$since ?e ?a ?v _ false]]
  ```

- **A superseded value,** with `$before` the database `AsOf(P)`:

  ```clojure
  [:find ?e ?a ?old ?new
   :in $ $since $before
   :where [$since ?e ?a ?new _ true]
          [$before ?e ?a ?old]
          [(!= ?old ?new)]]
  ```

- **The next iteration's work,** the delta joined with the facts that depend on it:

  ```clojure
  [:find ?reader :in $ $since :where [$since ?cell ?a ?v] [?reader :dep/reads ?cell]]
  ```

Each of these queries starts from the view, so each costs the datoms past P times a point lookup.
