package store

import (
	"testing"
)

func TestNewStore(t *testing.T) {
	store := NewStore()
	if store == nil {
		t.Fatal("NewStore() returned nil")
	}
	if store.data == nil {
		t.Fatal("Store data map is nil")
	}
}

func TestLen(t *testing.T) {
	store := NewStore()
	if store.Len() != 0 {
		t.Fatalf("Expected empty store, got %d keys", store.Len())
	}
	store.Put("a", []byte("1"))
	store.Put("b", []byte("2"))
	store.Put("a", []byte("3"))
	if store.Len() != 2 {
		t.Errorf("Expected 2 keys, got %d", store.Len())
	}
	store.Delete("a")
	if store.Len() != 1 {
		t.Errorf("Expected 1 key after delete, got %d", store.Len())
	}
}

func TestPut(t *testing.T) {
	store := NewStore()
	key := "test-key"
	value := []byte("test-value")

	store.Put(key, value)

	// Verify value was stored
	retrieved, ok := store.Get(key)
	if !ok {
		t.Fatal("Key not found after Put")
	}
	if string(retrieved) != string(value) {
		t.Errorf("Expected %s, got %s", string(value), string(retrieved))
	}
}

func TestGet(t *testing.T) {
	store := NewStore()
	key := "test-key"
	value := []byte("test-value")

	// Test non-existent key
	_, ok := store.Get(key)
	if ok {
		t.Error("Get() returned true for non-existent key")
	}

	// Store and retrieve
	store.Put(key, value)
	retrieved, ok := store.Get(key)
	if !ok {
		t.Fatal("Key not found after Put")
	}
	if string(retrieved) != string(value) {
		t.Errorf("Expected %s, got %s", string(value), string(retrieved))
	}
}

func TestDelete(t *testing.T) {
	store := NewStore()
	key := "test-key"
	value := []byte("test-value")

	// Test delete on non-existent key
	deleted := store.Delete(key)
	if deleted {
		t.Error("Delete() returned true for non-existent key")
	}

	// Store, then delete
	store.Put(key, value)
	deleted = store.Delete(key)
	if !deleted {
		t.Error("Delete() returned false for existing key")
	}

	// Verify key is gone
	_, ok := store.Get(key)
	if ok {
		t.Error("Key still exists after Delete")
	}
}

func TestConcurrentOperations(t *testing.T) {
	store := NewStore()
	key := "concurrent-key"
	value := []byte("concurrent-value")

	// Concurrent writes
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			store.Put(key, value)
			done <- true
		}()
	}

	// Wait for all writes
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify final state
	retrieved, ok := store.Get(key)
	if !ok {
		t.Fatal("Key not found after concurrent writes")
	}
	if string(retrieved) != string(value) {
		t.Errorf("Expected %s, got %s", string(value), string(retrieved))
	}
}

func TestDumpAndLoad(t *testing.T) {
	store := NewStore()
	
	// Add some data
	store.Put("key1", []byte("value1"))
	store.Put("key2", []byte("value2"))
	store.Put("key3", []byte("value3"))

	// Dump state
	state := store.Dump()
	if len(state) != 3 {
		t.Errorf("Expected 3 keys in dump, got %d", len(state))
	}

	// Create new store and load
	newStore := NewStore()
	newStore.Load(state)

	// Verify all keys are present
	val1, ok := newStore.Get("key1")
	if !ok || string(val1) != "value1" {
		t.Error("key1 not restored correctly")
	}

	val2, ok := newStore.Get("key2")
	if !ok || string(val2) != "value2" {
		t.Error("key2 not restored correctly")
	}

	val3, ok := newStore.Get("key3")
	if !ok || string(val3) != "value3" {
		t.Error("key3 not restored correctly")
	}
}

