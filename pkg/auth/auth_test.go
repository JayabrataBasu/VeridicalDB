package auth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUserCatalog_CreateUser(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Test creating a user (not superuser)
	err = catalog.CreateUser("testuser", "password123", false)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	// Verify user exists
	user, err := catalog.GetUser("testuser")
	if err != nil {
		t.Fatalf("failed to get user: %v", err)
	}
	if user.Username != "testuser" {
		t.Errorf("expected username 'testuser', got '%s'", user.Username)
	}
	if user.Superuser {
		t.Error("expected user not to be superuser")
	}

	// Test creating duplicate user
	err = catalog.CreateUser("testuser", "password456", false)
	if err == nil {
		t.Error("expected error creating duplicate user, got nil")
	}
}

func TestUserCatalog_DropUser(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Create a user
	err = catalog.CreateUser("testuser", "password123", false)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	// Drop the user
	err = catalog.DropUser("testuser", false)
	if err != nil {
		t.Fatalf("failed to drop user: %v", err)
	}

	// Verify user no longer exists
	_, err = catalog.GetUser("testuser")
	if err == nil {
		t.Error("expected error getting dropped user, got nil")
	}

	// Test dropping non-existent user without IF EXISTS
	err = catalog.DropUser("nonexistent", false)
	if err == nil {
		t.Error("expected error dropping non-existent user, got nil")
	}

	// Test dropping non-existent user with IF EXISTS
	err = catalog.DropUser("nonexistent", true)
	if err != nil {
		t.Error("expected no error with IF EXISTS for non-existent user")
	}
}

func TestUserCatalog_Authenticate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Create a user
	err = catalog.CreateUser("testuser", "password123", false)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	// Test successful authentication
	user, err := catalog.Authenticate("testuser", "password123")
	if err != nil {
		t.Fatalf("failed to authenticate: %v", err)
	}
	if user.Username != "testuser" {
		t.Errorf("expected username 'testuser', got '%s'", user.Username)
	}

	// Test wrong password
	_, err = catalog.Authenticate("testuser", "wrongpassword")
	if err == nil {
		t.Error("expected error with wrong password, got nil")
	}

	// Test non-existent user
	_, err = catalog.Authenticate("nonexistent", "password123")
	if err == nil {
		t.Error("expected error with non-existent user, got nil")
	}
}

func TestUserCatalog_AlterPassword(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Create a user
	err = catalog.CreateUser("testuser", "password123", false)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	// Alter password
	err = catalog.AlterPassword("testuser", "newpassword456")
	if err != nil {
		t.Fatalf("failed to alter password: %v", err)
	}

	// Verify old password no longer works
	_, err = catalog.Authenticate("testuser", "password123")
	if err == nil {
		t.Error("expected error with old password, got nil")
	}

	// Verify new password works
	_, err = catalog.Authenticate("testuser", "newpassword456")
	if err != nil {
		t.Fatalf("failed to authenticate with new password: %v", err)
	}
}

func TestUserCatalog_SetSuperuser(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Create a regular user
	err = catalog.CreateUser("testuser", "password123", false)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	// Promote to superuser
	err = catalog.SetSuperuser("testuser", true)
	if err != nil {
		t.Fatalf("failed to set superuser: %v", err)
	}

	// Verify superuser status changed
	user, err := catalog.GetUser("testuser")
	if err != nil {
		t.Fatalf("failed to get user: %v", err)
	}
	if !user.Superuser {
		t.Error("expected user to be superuser")
	}

	// Demote from superuser
	err = catalog.SetSuperuser("testuser", false)
	if err != nil {
		t.Fatalf("failed to unset superuser: %v", err)
	}

	user, err = catalog.GetUser("testuser")
	if err != nil {
		t.Fatalf("failed to get user: %v", err)
	}
	if user.Superuser {
		t.Error("expected user not to be superuser")
	}
}

func TestUserCatalog_GrantRevoke(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Create a user
	err = catalog.CreateUser("testuser", "password123", false)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	// Grant SELECT privilege
	err = catalog.Grant("testuser", "test_table", PrivSelect)
	if err != nil {
		t.Fatalf("failed to grant privilege: %v", err)
	}

	// Verify privilege
	if !catalog.HasPrivilege("testuser", "test_table", PrivSelect) {
		t.Error("expected user to have SELECT privilege")
	}
	if catalog.HasPrivilege("testuser", "test_table", PrivInsert) {
		t.Error("expected user to NOT have INSERT privilege")
	}

	// Grant more privileges
	err = catalog.Grant("testuser", "test_table", PrivInsert)
	if err != nil {
		t.Fatalf("failed to grant INSERT privilege: %v", err)
	}
	err = catalog.Grant("testuser", "test_table", PrivUpdate)
	if err != nil {
		t.Fatalf("failed to grant UPDATE privilege: %v", err)
	}

	// Verify all privileges
	if !catalog.HasPrivilege("testuser", "test_table", PrivSelect) {
		t.Error("expected user to have SELECT privilege")
	}
	if !catalog.HasPrivilege("testuser", "test_table", PrivInsert) {
		t.Error("expected user to have INSERT privilege")
	}
	if !catalog.HasPrivilege("testuser", "test_table", PrivUpdate) {
		t.Error("expected user to have UPDATE privilege")
	}

	// Revoke INSERT privilege
	err = catalog.Revoke("testuser", "test_table", PrivInsert)
	if err != nil {
		t.Fatalf("failed to revoke privilege: %v", err)
	}

	// Verify INSERT is revoked
	if catalog.HasPrivilege("testuser", "test_table", PrivInsert) {
		t.Error("expected user to NOT have INSERT privilege after revoke")
	}
	if !catalog.HasPrivilege("testuser", "test_table", PrivSelect) {
		t.Error("expected user to still have SELECT privilege")
	}
}

func TestUserCatalog_GrantAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Create a user
	err = catalog.CreateUser("testuser", "password123", false)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	// Grant ALL privilege
	err = catalog.Grant("testuser", "test_table", PrivAll)
	if err != nil {
		t.Fatalf("failed to grant ALL privilege: %v", err)
	}

	// Verify all privileges are granted via ALL
	if !catalog.HasPrivilege("testuser", "test_table", PrivSelect) {
		t.Error("expected user to have SELECT privilege with ALL")
	}
	if !catalog.HasPrivilege("testuser", "test_table", PrivInsert) {
		t.Error("expected user to have INSERT privilege with ALL")
	}
	if !catalog.HasPrivilege("testuser", "test_table", PrivUpdate) {
		t.Error("expected user to have UPDATE privilege with ALL")
	}
	if !catalog.HasPrivilege("testuser", "test_table", PrivDelete) {
		t.Error("expected user to have DELETE privilege with ALL")
	}
}

func TestUserCatalog_SuperuserHasAllPrivileges(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Create a superuser
	err = catalog.CreateUser("superuser", "password123", true)
	if err != nil {
		t.Fatalf("failed to create superuser: %v", err)
	}

	// Superuser should have all privileges on any table without explicit grant
	if !catalog.HasPrivilege("superuser", "any_table", PrivSelect) {
		t.Error("expected superuser to have SELECT privilege")
	}
	if !catalog.HasPrivilege("superuser", "any_table", PrivInsert) {
		t.Error("expected superuser to have INSERT privilege")
	}
	if !catalog.HasPrivilege("superuser", "any_table", PrivUpdate) {
		t.Error("expected superuser to have UPDATE privilege")
	}
	if !catalog.HasPrivilege("superuser", "any_table", PrivDelete) {
		t.Error("expected superuser to have DELETE privilege")
	}
}

func TestUserCatalog_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create a catalog and add users
	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	err = catalog.CreateUser("user1", "pass1", false)
	if err != nil {
		t.Fatalf("failed to create user1: %v", err)
	}
	err = catalog.CreateUser("user2", "pass2", true)
	if err != nil {
		t.Fatalf("failed to create user2: %v", err)
	}
	err = catalog.Grant("user1", "table1", PrivSelect)
	if err != nil {
		t.Fatalf("failed to grant SELECT: %v", err)
	}
	err = catalog.Grant("user1", "table1", PrivInsert)
	if err != nil {
		t.Fatalf("failed to grant INSERT: %v", err)
	}

	// Create a new catalog instance (will auto-load)
	catalog2, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create second catalog: %v", err)
	}

	// Verify users loaded correctly
	user1, err := catalog2.GetUser("user1")
	if err != nil {
		t.Fatalf("failed to get user1 from loaded catalog: %v", err)
	}
	if user1.Username != "user1" {
		t.Errorf("expected username 'user1', got '%s'", user1.Username)
	}
	if user1.Superuser {
		t.Error("expected user1 not to be superuser")
	}

	user2, err := catalog2.GetUser("user2")
	if err != nil {
		t.Fatalf("failed to get user2 from loaded catalog: %v", err)
	}
	if !user2.Superuser {
		t.Error("expected user2 to be superuser")
	}

	// Verify authentication still works after load
	_, err = catalog2.Authenticate("user1", "pass1")
	if err != nil {
		t.Fatalf("failed to authenticate after load: %v", err)
	}

	// Verify privileges loaded
	if !catalog2.HasPrivilege("user1", "table1", PrivSelect) {
		t.Error("expected user1 to have SELECT privilege after load")
	}
	if !catalog2.HasPrivilege("user1", "table1", PrivInsert) {
		t.Error("expected user1 to have INSERT privilege after load")
	}
}

func TestUserCatalog_ListUsers(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Create additional users
	err = catalog.CreateUser("user1", "pass1", false)
	if err != nil {
		t.Fatalf("failed to create user1: %v", err)
	}
	err = catalog.CreateUser("user2", "pass2", true)
	if err != nil {
		t.Fatalf("failed to create user2: %v", err)
	}

	// List users (will include default admin plus our users)
	users := catalog.ListUsers()
	if len(users) < 2 {
		t.Fatalf("expected at least 2 users, got %d", len(users))
	}

	// Verify user names (order may vary)
	userMap := make(map[string]bool)
	for _, name := range users {
		userMap[name] = true
	}
	if !userMap["user1"] || !userMap["user2"] {
		t.Error("expected both user1 and user2 in list")
	}
}

func TestPasswordHashing(t *testing.T) {
	password := "testpassword123"
	salt := "0001020304050607"

	// Hash password (legacy SHA256)
	hash := legacyHashPassword(password, salt)

	// Verify same password produces same hash
	hash2 := legacyHashPassword(password, salt)
	if hash != hash2 {
		t.Error("same password and salt should produce same hash")
	}

	// Verify different password produces different hash
	hash3 := legacyHashPassword("differentpassword", salt)
	if hash == hash3 {
		t.Error("different passwords should produce different hashes")
	}

	// Verify different salt produces different hash
	salt2 := "0102030405060708"
	hash4 := legacyHashPassword(password, salt2)
	if hash == hash4 {
		t.Error("different salts should produce different hashes")
	}
}

func TestUserCatalog_CheckAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Create a user with limited privileges
	err = catalog.CreateUser("testuser", "password123", false)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	// Grant only SELECT
	err = catalog.Grant("testuser", "test_table", PrivSelect)
	if err != nil {
		t.Fatalf("failed to grant privilege: %v", err)
	}

	// Check access should pass for SELECT
	err = catalog.CheckAccess("testuser", "test_table", PrivSelect)
	if err != nil {
		t.Errorf("expected CheckAccess to pass for SELECT: %v", err)
	}

	// Check access should fail for INSERT
	err = catalog.CheckAccess("testuser", "test_table", PrivInsert)
	if err == nil {
		t.Error("expected CheckAccess to fail for INSERT")
	}
}

func TestUserCatalog_DefaultAdmin(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create a fresh catalog - should create default admin
	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Default admin should exist
	admin, err := catalog.GetUser("admin")
	if err != nil {
		t.Fatalf("expected default admin user to exist: %v", err)
	}
	if !admin.Superuser {
		t.Error("expected default admin to be superuser")
	}

	// Default admin should NOT authenticate with literal 'admin' password
	_, err = catalog.Authenticate("admin", "admin")
	if err == nil {
		t.Error("did not expect default admin to authenticate with literal 'admin' password")
	}

	// Password should be stored using bcrypt (no legacy salt)
	if admin.Salt != "" {
		t.Errorf("expected admin salt to be empty for bcrypt users, got '%s'", admin.Salt)
	}
	if !strings.HasPrefix(admin.PasswordHash, "$2") {
		t.Errorf("expected bcrypt hash for admin, got '%s'", admin.PasswordHash)
	}
}

func TestUserCatalog_LegacyPasswordMigration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create a legacy user JSON file (SHA256 hex with salt)
	salt := "deadbeefcafebabe"
	legacyHash := legacyHashPassword("oldpass", salt)
	users := []*User{{Username: "legacy", PasswordHash: legacyHash, Salt: salt, Superuser: false, Privileges: map[string][]Priv{}}}
	data, _ := json.MarshalIndent(users, "", "  ")
	if err := os.WriteFile(filepath.Join(tmpDir, "users.json"), data, 0600); err != nil {
		t.Fatalf("failed to write users.json: %v", err)
	}

	catalog, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Authenticate should succeed and migrate password to bcrypt
	if _, err := catalog.Authenticate("legacy", "oldpass"); err != nil {
		t.Fatalf("expected legacy user to authenticate: %v", err)
	}

	// Reload and verify migration persisted
	catalog2, err := NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to reload catalog: %v", err)
	}
	u, err := catalog2.GetUser("legacy")
	if err != nil {
		t.Fatalf("expected migrated user to exist: %v", err)
	}
	if u.Salt != "" {
		t.Errorf("expected migrated user salt to be empty, got '%s'", u.Salt)
	}
	if !strings.HasPrefix(u.PasswordHash, "$2") {
		t.Errorf("expected migrated bcrypt hash, got '%s'", u.PasswordHash)
	}
}

func TestUserCatalog_FilePath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "auth_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	_, err = NewUserCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Verify file was created
	usersFile := filepath.Join(tmpDir, "users.json")
	if _, err := os.Stat(usersFile); os.IsNotExist(err) {
		t.Error("expected users.json file to be created")
	}
}
