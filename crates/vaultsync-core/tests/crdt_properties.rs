//! CRDT property-based tests. (vaultsync-testing-spec.md §6)
//! Tests 4 CRDT properties using randomly generated mutations.

use vaultsync_core::crdt::{document::CRDTDocument, types::CrdtValue};
use proptest::prelude::*;

fn new_doc() -> CRDTDocument {
    CRDTDocument::new("test", "r:1", 0)
}

fn arb_value() -> impl Strategy<Value = CrdtValue> {
    prop_oneof![
        "[a-z]{0,20}".prop_map(CrdtValue::String),
        any::<f64>().prop_filter("finite", |f| f.is_finite()).prop_map(CrdtValue::Number),
        any::<bool>().prop_map(CrdtValue::Boolean),
        Just(CrdtValue::Null),
    ]
}

fn arb_mutation() -> impl Strategy<Value = (String, CrdtValue)> {
    ("[a-z]{1,8}", arb_value())
}

fn arb_mutations() -> impl Strategy<Value = Vec<(String, CrdtValue)>> {
    proptest::collection::vec(arb_mutation(), 1..15)
}

fn apply_all(doc: &mut CRDTDocument, ms: &[(String, CrdtValue)]) -> Vec<Vec<u8>> {
    ms.iter().map(|(k, v)| doc.set_field(k, v.clone())).collect()
}

fn permuted<T: Clone>(items: &[T], seed: u64) -> Vec<T> {
    let mut v = items.to_vec();
    let mut s = seed;
    for i in (1..v.len()).rev() {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let j = (s >> 33) as usize % (i + 1);
        v.swap(i, j);
    }
    v
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    // ── Property 1: Convergence ──────────────────────────────────────────────────
    #[test]
    fn prop_convergence(mutations in arb_mutations(), seed in any::<u64>()) {
        let base = new_doc();
        let snapshot = base.to_snapshot();

        let mut doc_a = CRDTDocument::from_snapshot(&snapshot).unwrap();
        let mut doc_b = CRDTDocument::from_snapshot(&snapshot).unwrap();

        let ua = apply_all(&mut doc_a, &mutations);
        let ub = apply_all(&mut doc_b, &permuted(&mutations, seed));

        for u in &ub {
            doc_a.apply_update(u).unwrap();
        }
        for u in &ua {
            doc_b.apply_update(u).unwrap();
        }

        prop_assert_eq!(doc_a.to_map(), doc_b.to_map(),
            "Convergence failed: mutations in different order → different state");
    }

    // ── Property 2: Commutativity ─────────────────────────────────────────────
    #[test]
    fn prop_commutativity(m1 in arb_mutation(), m2 in arb_mutation()) {
        let base = new_doc();
        let snapshot = base.to_snapshot();

        let mut branch_a = CRDTDocument::from_snapshot(&snapshot).unwrap();
        let mut branch_b = CRDTDocument::from_snapshot(&snapshot).unwrap();

        let u1 = branch_a.set_field(&m1.0, m1.1.clone());
        let u2 = branch_b.set_field(&m2.0, m2.1.clone());

        let mut ab = CRDTDocument::from_snapshot(&snapshot).unwrap();
        ab.apply_update(&u1).unwrap();
        ab.apply_update(&u2).unwrap();

        let mut ba = CRDTDocument::from_snapshot(&snapshot).unwrap();
        ba.apply_update(&u2).unwrap();
        ba.apply_update(&u1).unwrap();

        prop_assert_eq!(ab.to_map(), ba.to_map(),
            "Commutativity failed: A then B ≠ B then A");
    }

    // ── Property 3: Associativity ─────────────────────────────────────────────
    #[test]
    fn prop_associativity(
        ms in proptest::collection::vec(arb_mutation(), 3..12)
    ) {
        let base = new_doc();
        let snapshot = base.to_snapshot();

        let third = ms.len() / 3;
        let (ma, rest) = ms.split_at(third);
        let (mb, mc) = rest.split_at(rest.len() / 2);

        let mut da = CRDTDocument::from_snapshot(&snapshot).unwrap();
        let ua = apply_all(&mut da, ma);
        let mut db = CRDTDocument::from_snapshot(&snapshot).unwrap();
        let ub = apply_all(&mut db, mb);
        let mut dc = CRDTDocument::from_snapshot(&snapshot).unwrap();
        let uc = apply_all(&mut dc, mc);

        // Grouping 1: (da merge db) merge dc
        let mut doc_ab = CRDTDocument::from_snapshot(&snapshot).unwrap();
        for u in &ua { doc_ab.apply_update(u).unwrap(); }
        for u in &ub { doc_ab.apply_update(u).unwrap(); }
        let snap_ab = doc_ab.to_snapshot();
        let mut doc_ab_c = CRDTDocument::from_snapshot(&snap_ab).unwrap();
        for u in &uc { doc_ab_c.apply_update(u).unwrap(); }

        // Grouping 2: da merge (db merge dc)
        let mut doc_bc = CRDTDocument::from_snapshot(&snapshot).unwrap();
        for u in &ub { doc_bc.apply_update(u).unwrap(); }
        for u in &uc { doc_bc.apply_update(u).unwrap(); }
        let snap_bc = doc_bc.to_snapshot();
        let mut doc_a_bc = CRDTDocument::from_snapshot(&snapshot).unwrap();
        for u in &ua { doc_a_bc.apply_update(u).unwrap(); }
        doc_a_bc.apply_update(&snap_bc).unwrap();

        prop_assert_eq!(doc_ab_c.to_map(), doc_a_bc.to_map(),
            "Associativity failed");
    }

    // ── Property 4: Idempotence ───────────────────────────────────────────────
    #[test]
    fn prop_idempotence(mutation in arb_mutation()) {
        let base = new_doc();
        let snapshot = base.to_snapshot();

        let mut doc = CRDTDocument::from_snapshot(&snapshot).unwrap();
        let update = doc.set_field(&mutation.0, mutation.1);

        let mut test_doc = CRDTDocument::from_snapshot(&snapshot).unwrap();
        test_doc.apply_update(&update).unwrap();
        let once = test_doc.to_map();

        test_doc.apply_update(&update).unwrap();
        let twice = test_doc.to_map();

        prop_assert_eq!(once, twice,
            "Idempotence failed: applying same update twice changed state");
    }
}
