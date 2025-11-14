import sys
import os
from datetime import datetime
import pandas as pd

# Add src to path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from settings import settings
from utils.db_client import DatabaseClient


class DatabaseClientIntegrationTest:
    def __init__(self):
        self.db_client = settings.database_client
        self.test_table = f"test_db_client_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.results = {
            'passed': [],
            'failed': []
        }
    
    def log(self, message, level='INFO'):
        """Log a message with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] [{level}] {message}")
    
    def test_execute_query_create_table(self):
        """Test 1: Create a test table using execute_query"""
        self.log("TEST 1: Creating test table using execute_query()")
        
        try:
            create_table_sql = f"""
            CREATE TABLE {self.test_table} (
                id INTEGER,
                name VARCHAR(100),
                value DECIMAL(10, 2),
                created_at TIMESTAMP,
                is_active BOOLEAN
            )
            """
            
            self.db_client.execute_query(create_table_sql)
            self.log(f"✓ Successfully created table: {self.test_table}", 'SUCCESS')
            self.results['passed'].append('execute_query (CREATE TABLE)')
            return True
        except Exception as e:
            self.log(f"✗ Failed to create table: {e}", 'ERROR')
            self.results['failed'].append(f'execute_query (CREATE TABLE): {e}')
            return False
    
    def test_write_df_insert_data(self):
        """Test 2: Insert data using write_df"""
        self.log("TEST 2: Inserting data using write_df()")
        
        try:
            # Create test DataFrame
            test_data = pd.DataFrame({
                'id': [1, 2, 3, 4, 5],
                'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
                'value': [100.50, 250.75, 300.00, 150.25, 400.99],
                'created_at': [datetime.now()] * 5,
                'is_active': [True, True, False, True, False]
            })
            
            self.db_client.write_df(test_data, self.test_table, if_exists='append')
            self.log(f"✓ Successfully inserted {len(test_data)} rows", 'SUCCESS')
            self.results['passed'].append('write_df (INSERT)')
            return True
        except Exception as e:
            self.log(f"✗ Failed to insert data: {e}", 'ERROR')
            self.results['failed'].append(f'write_df (INSERT): {e}')
            return False
    
    def test_read_sql_select_all(self):
        """Test 3: Read all data using read_sql"""
        self.log("TEST 3: Reading all data using read_sql()")
        
        try:
            df = self.db_client.read_sql(f"SELECT * FROM {self.test_table} ORDER BY id")
            
            self.log(f"✓ Successfully read {len(df)} rows", 'SUCCESS')
            self.log(f"  Columns: {list(df.columns)}")
            self.log(f"  First row: {df.iloc[0].to_dict()}")
            
            # Validate data
            assert len(df) == 5, f"Expected 5 rows, got {len(df)}"
            assert 'id' in df.columns, "Missing 'id' column"
            assert 'name' in df.columns, "Missing 'name' column"
            
            self.results['passed'].append('read_sql (SELECT ALL)')
            return True
        except Exception as e:
            self.log(f"✗ Failed to read data: {e}", 'ERROR')
            self.results['failed'].append(f'read_sql (SELECT ALL): {e}')
            return False
    
    def test_read_sql_with_filter(self):
        """Test 4: Read filtered data using read_sql"""
        self.log("TEST 4: Reading filtered data using read_sql()")
        
        try:
            df = self.db_client.read_sql(
                f"SELECT * FROM {self.test_table} WHERE is_active = true ORDER BY id"
            )
            
            self.log(f"✓ Successfully read {len(df)} active rows", 'SUCCESS')
            assert len(df) == 3, f"Expected 3 active rows, got {len(df)}"
            
            self.results['passed'].append('read_sql (SELECT with WHERE)')
            return True
        except Exception as e:
            self.log(f"✗ Failed to read filtered data: {e}", 'ERROR')
            self.results['failed'].append(f'read_sql (SELECT with WHERE): {e}')
            return False
    
    def test_fetch_one(self):
        """Test 5: Fetch single row using fetch_one"""
        self.log("TEST 5: Fetching single row using fetch_one()")
        
        try:
            row = self.db_client.fetch_one(
                f"SELECT id, name, value FROM {self.test_table} WHERE id = 1"
            )
            
            self.log(f"✓ Successfully fetched row: {row}", 'SUCCESS')
            assert row is not None, "Expected a row, got None"
            assert row[0] == 1, f"Expected id=1, got {row[0]}"
            
            self.results['passed'].append('fetch_one')
            return True
        except Exception as e:
            self.log(f"✗ Failed to fetch one: {e}", 'ERROR')
            self.results['failed'].append(f'fetch_one: {e}')
            return False
    
    def test_fetch_all(self):
        """Test 6: Fetch all rows using fetch_all"""
        self.log("TEST 6: Fetching all rows using fetch_all()")
        
        try:
            rows = self.db_client.fetch_all(
                f"SELECT id, name FROM {self.test_table} ORDER BY id"
            )
            
            self.log(f"✓ Successfully fetched {len(rows)} rows", 'SUCCESS')
            self.log(f"  First row: {rows[0]}")
            self.log(f"  Last row: {rows[-1]}")
            
            assert len(rows) == 5, f"Expected 5 rows, got {len(rows)}"
            
            self.results['passed'].append('fetch_all')
            return True
        except Exception as e:
            self.log(f"✗ Failed to fetch all: {e}", 'ERROR')
            self.results['failed'].append(f'fetch_all: {e}')
            return False
    
    def test_execute_scalar(self):
        """Test 7: Get scalar value using execute_scalar"""
        self.log("TEST 7: Getting count using execute_scalar()")
        
        try:
            count = self.db_client.execute_scalar(
                f"SELECT COUNT(*) FROM {self.test_table}"
            )
            
            self.log(f"✓ Successfully got count: {count}", 'SUCCESS')
            assert count == 5, f"Expected count=5, got {count}"
            
            self.results['passed'].append('execute_scalar')
            return True
        except Exception as e:
            self.log(f"✗ Failed to execute scalar: {e}", 'ERROR')
            self.results['failed'].append(f'execute_scalar: {e}')
            return False
    
    def test_execute_scalar_exists(self):
        """Test 8: Check table exists using execute_scalar"""
        self.log("TEST 8: Checking table existence using execute_scalar()")
        
        try:
            exists = self.db_client.execute_scalar(f"""
                SELECT EXISTS (
                    SELECT * FROM information_schema.tables 
                    WHERE table_name = '{self.test_table}'
                )
            """)
            
            self.log(f"✓ Table exists check: {exists}", 'SUCCESS')
            assert exists is True, f"Expected True, got {exists}"
            
            self.results['passed'].append('execute_scalar (EXISTS)')
            return True
        except Exception as e:
            self.log(f"✗ Failed exists check: {e}", 'ERROR')
            self.results['failed'].append(f'execute_scalar (EXISTS): {e}')
            return False
    
    def cleanup(self):
        """Clean up: Drop the test table"""
        self.log("CLEANUP: Dropping test table")
        
        try:
            self.db_client.execute_query(f"DROP TABLE IF EXISTS {self.test_table}")
            self.log(f"✓ Successfully dropped table: {self.test_table}", 'SUCCESS')
            return True
        except Exception as e:
            self.log(f"✗ Failed to drop table: {e}", 'ERROR')
            return False
    
    def print_summary(self):
        """Print test summary"""
        self.log("=" * 70)
        self.log("TEST SUMMARY")
        self.log("=" * 70)
        
        total = len(self.results['passed']) + len(self.results['failed'])
        passed = len(self.results['passed'])
        failed = len(self.results['failed'])
        
        self.log(f"Total tests: {total}")
        self.log(f"Passed: {passed}", 'SUCCESS')
        
        if failed > 0:
            self.log(f"Failed: {failed}", 'ERROR')
            self.log("\nFailed tests:")
            for test in self.results['failed']:
                self.log(f"  - {test}", 'ERROR')
        
        self.log("=" * 70)
        
        if failed == 0:
            self.log("🎉 ALL TESTS PASSED! 🎉", 'SUCCESS')
        else:
            self.log("❌ SOME TESTS FAILED ❌", 'ERROR')
    
    def run_all_tests(self):
        """Run all integration tests"""
        self.log("=" * 70)
        self.log("Starting DatabaseClient Integration Tests")
        self.log("=" * 70)
        self.log(f"Test table: {self.test_table}")
        self.log("=" * 70)
        
        try:
            # Run tests in sequence
            tests = [
                self.test_execute_query_create_table,
                self.test_write_df_insert_data,
                self.test_read_sql_select_all,
                self.test_read_sql_with_filter,
                self.test_fetch_one,
                self.test_fetch_all,
                self.test_execute_scalar,
                self.test_execute_scalar_exists,
            ]
            
            for test in tests:
                success = test()
                if not success:
                    self.log(f"Test failed, continuing with remaining tests...", 'WARNING')
                self.log("")  # Empty line for readability
            
        finally:
            # Always try to cleanup
            self.cleanup()
            self.log("")
            self.print_summary()


def main():
    """Main entry point"""
    try:
        test_suite = DatabaseClientIntegrationTest()
        test_suite.run_all_tests()
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

