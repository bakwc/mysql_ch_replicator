"""Cross-table referential integrity validation tests"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME


class TestReferentialIntegrity(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test referential integrity across multiple tables during replication"""

    @pytest.mark.integration
    def test_foreign_key_relationship_replication(self):
        """Test foreign key relationships are maintained during replication"""
        # Create parent table (users)
        self.mysql.execute("""
        CREATE TABLE users (
            user_id int NOT NULL AUTO_INCREMENT,
            username varchar(50) UNIQUE NOT NULL,
            email varchar(100) UNIQUE NOT NULL,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id)
        );
        """)

        # Create child table (orders) with foreign key
        self.mysql.execute("""
        CREATE TABLE orders (
            order_id int NOT NULL AUTO_INCREMENT,
            user_id int NOT NULL,
            order_amount decimal(10,2) NOT NULL,
            order_date timestamp DEFAULT CURRENT_TIMESTAMP,
            status varchar(20) DEFAULT 'pending',
            PRIMARY KEY (order_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );
        """)

        # Insert parent records first
        users_data = [
            {"username": "alice", "email": "alice@example.com"},
            {"username": "bob", "email": "bob@example.com"},
            {"username": "charlie", "email": "charlie@example.com"}
        ]
        self.insert_multiple_records("users", users_data)

        # Get user IDs for foreign key references
        with self.mysql.get_connection() as (connection, cursor):
            cursor.execute("SELECT user_id, username FROM users ORDER BY user_id")
            user_mappings = {row[1]: row[0] for row in cursor.fetchall()}

        # Insert child records with valid foreign keys BEFORE starting replication
        orders_data = [
            {"user_id": user_mappings["alice"], "order_amount": 99.99, "status": "completed"},
            {"user_id": user_mappings["bob"], "order_amount": 149.50, "status": "pending"},
            {"user_id": user_mappings["alice"], "order_amount": 79.99, "status": "completed"},
            {"user_id": user_mappings["charlie"], "order_amount": 199.99, "status": "shipped"}
        ]
        self.insert_multiple_records("orders", orders_data)

        # Start replication AFTER all data is inserted
        self.start_replication()
        self.wait_for_table_sync("users", expected_count=3)
        self.wait_for_table_sync("orders", expected_count=4)

        # Verify referential integrity in ClickHouse
        self._verify_foreign_key_integrity("users", "orders", "user_id")

        # Test cascading updates (if supported)
        self.mysql.execute(
            "UPDATE users SET email = 'alice.new@example.com' WHERE username = 'alice'",
            commit=True
        )

        # Verify update propagated
        self.wait_for_record_update("users", "username='alice'", {"email": "alice.new@example.com"})

        # Verify child records still reference correct parent
        alice_orders = self.ch.select("orders", where=f"user_id={user_mappings['alice']}")
        assert len(alice_orders) == 2, "Alice should have 2 orders"

    @pytest.mark.integration
    def test_multi_table_transaction_integrity(self):
        """Test transaction integrity across multiple related tables"""
        # Create inventory and transaction tables
        self.mysql.execute("""
        CREATE TABLE inventory (
            item_id int NOT NULL AUTO_INCREMENT,
            item_name varchar(100) NOT NULL,
            quantity int NOT NULL DEFAULT 0,
            price decimal(10,2) NOT NULL,
            PRIMARY KEY (item_id)
        );
        """)

        self.mysql.execute("""
        CREATE TABLE transactions (
            txn_id int NOT NULL AUTO_INCREMENT,
            item_id int NOT NULL,
            quantity_changed int NOT NULL,
            txn_type enum('purchase','sale','adjustment'),
            txn_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (txn_id),
            FOREIGN KEY (item_id) REFERENCES inventory(item_id)
        );
        """)

        # Insert initial inventory
        inventory_data = [
            {"item_name": "Widget A", "quantity": 100, "price": 19.99},
            {"item_name": "Widget B", "quantity": 50, "price": 29.99},
            {"item_name": "Widget C", "quantity": 75, "price": 39.99}
        ]
        self.insert_multiple_records("inventory", inventory_data)

        # Perform multi-table transaction operations BEFORE starting replication
        transaction_scenarios = [
            # Purchase - increase inventory, record transaction
            {
                "item_name": "Widget A",
                "quantity_change": 25,
                "txn_type": "purchase",
                "new_quantity": 125
            },
            # Sale - decrease inventory, record transaction
            {
                "item_name": "Widget B", 
                "quantity_change": -15,
                "txn_type": "sale",
                "new_quantity": 35
            },
            # Adjustment - correct inventory, record transaction
            {
                "item_name": "Widget C",
                "quantity_change": -5,
                "txn_type": "adjustment", 
                "new_quantity": 70
            }
        ]

        for scenario in transaction_scenarios:
            # Execute as atomic transaction within a single connection
            with self.mysql.get_connection() as (connection, cursor):
                # Begin transaction
                cursor.execute("BEGIN")
                
                # Get item_id
                cursor.execute(
                    "SELECT item_id FROM inventory WHERE item_name = %s",
                    (scenario["item_name"],)
                )
                item_id = cursor.fetchone()[0]
                
                # Update inventory
                cursor.execute(
                    "UPDATE inventory SET quantity = %s WHERE item_id = %s",
                    (scenario["new_quantity"], item_id)
                )
                
                # Record transaction
                cursor.execute(
                    "INSERT INTO transactions (item_id, quantity_changed, txn_type) VALUES (%s, %s, %s)",
                    (item_id, scenario["quantity_change"], scenario["txn_type"])
                )
                
                # Commit transaction
                cursor.execute("COMMIT")
                connection.commit()

        # Start replication AFTER all transactions are complete
        self.start_replication()
        self.wait_for_table_sync("inventory", expected_count=3)
        self.wait_for_table_sync("transactions", expected_count=3)

        # Verify transaction integrity
        self._verify_inventory_transaction_consistency()

    def _verify_foreign_key_integrity(self, parent_table, child_table, fk_column):
        """Verify foreign key relationships are maintained in replicated data"""
        # Get all parent IDs
        parent_records = self.ch.select(parent_table)
        parent_ids = {record[f"{parent_table[:-1]}_id"] for record in parent_records}
        
        # Get all child foreign keys
        child_records = self.ch.select(child_table)
        child_fk_ids = {record[fk_column] for record in child_records}
        
        # Verify all foreign keys reference existing parents
        invalid_fks = child_fk_ids - parent_ids
        assert len(invalid_fks) == 0, f"Invalid foreign keys found: {invalid_fks}"
        
        # Verify referential counts match expectations
        for parent_id in parent_ids:
            mysql_child_count = self._get_mysql_child_count(child_table, fk_column, parent_id)
            ch_child_count = len(self.ch.select(child_table, where=f"{fk_column}={parent_id}"))
            assert mysql_child_count == ch_child_count, (
                f"Child count mismatch for {fk_column}={parent_id}: "
                f"MySQL={mysql_child_count}, ClickHouse={ch_child_count}"
            )

    def _get_mysql_child_count(self, child_table, fk_column, parent_id):
        """Get child record count from MySQL"""
        with self.mysql.get_connection() as (connection, cursor):
            cursor.execute(f"SELECT COUNT(*) FROM {child_table} WHERE {fk_column} = %s", (parent_id,))
            return cursor.fetchone()[0]

    def _verify_inventory_transaction_consistency(self):
        """Verify inventory quantities match transaction history"""
        # Get current inventory from both systems
        mysql_inventory = {}
        with self.mysql.get_connection() as (connection, cursor):
            cursor.execute("SELECT item_id, item_name, quantity FROM inventory")
            for item_id, name, qty in cursor.fetchall():
                mysql_inventory[item_id] = {"name": name, "quantity": qty}

        ch_inventory = {}
        for record in self.ch.select("inventory"):
            ch_inventory[record["item_id"]] = {
                "name": record["item_name"],
                "quantity": record["quantity"]
            }

        # Verify inventory matches
        assert mysql_inventory == ch_inventory, "Inventory mismatch between MySQL and ClickHouse"

        # Verify transaction totals make sense
        for item_id in mysql_inventory.keys():
            mysql_txn_total = self._get_mysql_transaction_total(item_id)
            ch_txn_total = self._get_ch_transaction_total(item_id)
            assert mysql_txn_total == ch_txn_total, (
                f"Transaction total mismatch for item {item_id}: "
                f"MySQL={mysql_txn_total}, ClickHouse={ch_txn_total}"
            )

    def _get_mysql_transaction_total(self, item_id):
        """Get transaction total for item from MySQL"""
        with self.mysql.get_connection() as (connection, cursor):
            cursor.execute("SELECT SUM(quantity_changed) FROM transactions WHERE item_id = %s", (item_id,))
            result = cursor.fetchone()[0]
            return result if result is not None else 0

    def _get_ch_transaction_total(self, item_id):
        """Get transaction total for item from ClickHouse"""
        transactions = self.ch.select("transactions", where=f"item_id={item_id}")
        return sum(txn["quantity_changed"] for txn in transactions)