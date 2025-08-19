use epoch_db::DB;
use std::error::Error;
use tempfile::tempdir;

/// This example demonstrates how to use the high-level transaction API
/// to perform an atomic "transfer" operation between two keys.
fn main() -> Result<(), Box<dyn Error>> {
    // 1. Setup a temporary database for the example.
    let temp_dir = tempdir()?;
    let db = DB::new(temp_dir.path())?;

    println!("Database created successfully.");

    // 2. Set the initial state for two user accounts.
    db.set("user:alice", "100", None)?;
    db.set("user:bob", "50", None)?;

    println!("Initial state:");
    println!("  - Alice's balance: {}", db.get("user:alice")?.unwrap());
    println!("  - Bob's balance:   {}", db.get("user:bob")?.unwrap());
    println!("\nAttempting to transfer 20 from Alice to Bob...");

    // 3. Run the atomic transfer inside a transaction.
    let transaction_result = db.transaction(|tx| {
        // Get the current balances.
        let alice_balance_str = tx.get("user:alice")?.unwrap();
        let bob_balance_str = tx.get("user:bob")?.unwrap();

        let mut alice_balance: u64 = alice_balance_str.parse()?;
        let mut bob_balance: u64 = bob_balance_str.parse()?;

        let transfer_amount = 20;

        // Check if the transfer is possible.
        if alice_balance < transfer_amount {
            // By returning an error, we abort the entire transaction.
            // No changes will be saved.
            return Err(Box::from("Alice has insufficient funds!"));
        }

        // Perform the transfer.
        alice_balance -= transfer_amount;
        bob_balance += transfer_amount;

        // Save the new balances.
        tx.set("user:alice", &alice_balance.to_string(), None)?;
        tx.set("user:bob", &bob_balance.to_string(), None)?;

        // Return Ok to commit the transaction.
        Ok(())
    });

    // 4. Check if the transaction succeeded.
    if let Err(e) = transaction_result {
        println!("\nTransaction failed: {}", e);
    } else {
        println!("\nTransaction successful!");
    }

    // 5. Verify the final state of the database.
    // The balances should be updated because the transaction succeeded.
    println!("\nFinal state:");
    println!("  - Alice's balance: {}", db.get("user:alice")?.unwrap());
    println!("  - Bob's balance:   {}", db.get("user:bob")?.unwrap());

    assert_eq!(db.get("user:alice")?.unwrap(), "80");
    assert_eq!(db.get("user:bob")?.unwrap(), "70");

    Ok(())
}
